#encoding: utf-8

require 'rubygems'
require 'bundler/setup'

require 'csv'
require 'rest_client'
require 'base64'
require 'yaml'
require 'json'
require 'getoptlong'
require 'marc'

debug = false
publish_during_save = false

opts = GetoptLong.new(
  [ '--help', '-h', GetoptLong::NO_ARGUMENT ],
  [ '--debug', GetoptLong::OPTIONAL_ARGUMENT ],
	[ '--publish', GetoptLong::OPTIONAL_ARGUMENT ],
)
opts.each do |opt, arg|
  case opt
    when '--help'
			puts <<-EOF
hyacinth_publish_target_clio_sync.rb [OPTIONS]

-h, --help:
   show help

--debug:
   run in debug mode with additional output

--publish:
   in addition to the default behavior of updating publish targets, also publish all updated publish targets
      EOF
		when '--debug'
			debug = true
		when '--publish'
			publish_during_save = true
	end
end

config_file_path = File.join(__dir__, 'config.yml')

unless File.exists?(config_file_path)
  puts 'Error: Missing required config file: config.yml'
  exit
end

config = YAML::load_file(config_file_path)
# Note: Base64.encode64 method can include newline characters, which messes things up.  That's why we're using Base64.strict_encode64 instead.
hyacinth_basic_auth_token = Base64.strict_encode64(config['hyacinth_email'] + ':' + config['hyacinth_password'])

# Step 1: Get PIDs and CLIO IDs for all publish targets that have CLIO IDs
hyacinth_search_url = "#{config['hyacinth_url']}/digital_objects/search.json"
post_params = {
	search: {
		per_page: 1000, # Get ALL publish targets.  There aren't even close to 1000 of them.
		f: {
			digital_object_type_display_label_sim: ['Publish Target']
		}
	}
}

begin
  hyacinth_search_response = RestClient::Request.execute(
    method: :post,
    url: hyacinth_search_url,
    timeout: 120,
    payload: post_params,
    headers: {Authorization: "Basic #{hyacinth_basic_auth_token}"}
  )
rescue RestClient::ExceptionWithResponse => err
  puts "Error: Received response '#{err.message}' for Hyacinth search request."
	exit
end

search_result_json = JSON.parse(hyacinth_search_response.body)

if search_result_json['total'] == 0
	puts "Error: No publish targets found."
	exit
end

pids_to_clio_ids_to_sync = {}

puts "Total Publish Target objects found: #{search_result_json['total']}" if debug
puts "About to process: #{search_result_json['results'].length}" if debug

search_result_json['results'].each do |hyacinth_search_result|
	pid = hyacinth_search_result['pid']
	digital_object_data = JSON.parse(hyacinth_search_result['digital_object_data_ts'])
	dynamic_field_data = digital_object_data['dynamic_field_data']

	clio_identifier_field = dynamic_field_data.fetch('clio_identifier', [{}])
	if dynamic_field_data['clio_identifier'].nil?
		next # Skip this publish target because it has no associated CLIO record
	elsif dynamic_field_data['clio_identifier'].length > 1
		puts "Error: Skipping sync of Hyacinth object with PID #{pid} because it has multiple CLIO identifiers, but we only expected one."
	end
	clio_id = dynamic_field_data['clio_identifier'].first['clio_identifier_value']
	pids_to_clio_ids_to_sync[pid] = clio_id
end

puts "Number of Publish Targets with CLIO IDs: #{pids_to_clio_ids_to_sync.length}" if debug

# For each publish target with CLIO ID, get the MARC record for that CLIO ID from CLIO
pids_to_clio_ids_to_sync.each do |pid, clio_id|
	puts "--- Processing #{pid} (CLIO ID: #{clio_id}) ---" if debug
  begin
    marc_data_response = RestClient::Request.execute(
      method: :get,
      max_redirects: 0, # Don't automatically follow redirects
      url: "https://clio.columbia.edu/catalog/#{clio_id}.marc",
      timeout: 120
    )
    # Don't automatically follow redirects
  rescue RestClient::ExceptionWithResponse => err
    puts "Error: Received response '#{err.message}' for CLIO record MARC21 request. CLIO ID: #{clio_id}, Hyacinth pid: #{pid}"
    next
  end
  sleep 0.5 # Need to sleep after each request to CLIO, otherwise CLIO may return a '429 Too Many Requests' response
	marc_record = MARC::Record.new_from_marc(marc_data_response.body)
	num_nonsort_characters = marc_record['245'].indicator2.to_i

	title = marc_record['245']['a']
	summary = marc_record['520']['a']

	# Remove certain trailing characters from the title, as well as whitespace
	puts "Title before cleanup: #{title}" if debug
	title = title.gsub(/[\.\/,:]$/, '').strip
	puts "Title after cleanup: #{title}" if debug

	title_non_sort_portion = title[0...num_nonsort_characters]
	title_sort_portion = title[num_nonsort_characters..-1]


	puts "Num nonsort chars: #{num_nonsort_characters}" if debug
	puts "Title Non-Sort Portion: #{title_non_sort_portion}" if debug
	puts "Title Sort Portion: #{title_sort_portion}" if debug
	puts "Summary: #{summary}" if debug

	hyacinth_record_update_url = "https://hyacinth.library.columbia.edu/digital_objects/#{pid}.json"
	put_params = {
		publish: publish_during_save,
		digital_object_data_json: JSON.generate({
			dynamic_field_data: {
				title: [
					{
						title_non_sort_portion: title_non_sort_portion,
						title_sort_portion: title_sort_portion
					}
				]
			},
			publish_target_data: {
				short_description: summary,
				full_description: summary
				# TODO: Later, we'll be updating the short_description field only, and using
				# the full_description field as an extension of the short one (and probably
				# renaming full_description to something like additional_description)
			}
		})
	}
  begin
    hyacinth_record_update_response = RestClient::Request.execute(
      method: :put,
      url: hyacinth_record_update_url,
      timeout: 120,
      payload: put_params,
      headers: {Authorization: "Basic #{hyacinth_basic_auth_token}"}
    )
  rescue RestClient::ExceptionWithResponse => err
    puts "Error: Received response '#{err.message}' for Hyacinth record update request for #{pid}"
    next
  end

	puts "#{pid} updated successfully" + (publish_during_save ? ' AND published' : '') if debug
end

puts 'Done!' if debug
