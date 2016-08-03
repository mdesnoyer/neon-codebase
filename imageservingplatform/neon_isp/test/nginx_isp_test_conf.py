# NGINX configuration file template

conf=" daemon off; \
error_log  %s;  events {     worker_connections  1024; }\
http {     default_type  application/octet-stream; \
    sendfile        on;     keepalive_timeout  65;      \
    mastermind_file_url %s; \
    s3downloader  %s; \
    s3port %s; \
    mastermind_validated_filepath %s; \
    mastermind_download_filepath %s;\
    updater_sleep_interval 60;      \
        server {  listen       %s; \
                server_name localhost; \
                proxy_set_header Host $host;\
                proxy_set_header X-Real-IP $remote_addr;\
                proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;\
        location ~ ^/v1/client/(.+\.*)$ { \
            v1_client; 		} \
        location ~ ^/v1/server/(.+\.*)$ { \
            v1_server; 		}\
        location ~ ^/v1/video/?$ { \
            set_unescape_uri $args; proxy_pass http://localhost:80/v1/video?$args; v1_video;}\
        location ~ ^/v1/getthumbnailid/(.+\.*)$ { \
            v1_getthumbnailid; 		}\
        location = /stats { \
            mastermind_stats; 		}  		\
        location = /healthcheck { \
            mastermind_healthcheck; 		}\
    }\
} " 
