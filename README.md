# hyacinth_publish_target_clio_sync
A script for syncing CLIO data to Hyacinth for Publish Targets

## How to run the script:

1. Create config.yml file and place it in the same directory as hyacinth_publish_target_clio_sync.rb
2. Run `ruby hyacinth_publish_target_clio_sync.rb` (for error-level output) or `ruby hyacinth_publish_target_clio_sync.rb --debug` (for debug-level output).

## config.yml example:
```
hyacinth_url: https://www.my-hyacinth-app-url.com
hyacinth_email: myemail@example.com
hyacinth_password: mypassword
```
