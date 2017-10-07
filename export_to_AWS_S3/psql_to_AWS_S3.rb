=begin
Ruby v. 2.3.3

Using bundler
$ gem install bundler

To compile the Ruby pg module, we also need the development files of the C libpg library.
$ sudo apt-get install libpq-dev

require 'pg'
$ gem install pg

This application writes .csv(output) / .txt(error) files from a postgres-db
named by date, by redirecting stdout to a .csv file and stderr to a .txt file.

You'll need to replace the path for the output.
Environment variables in .envrc file (using direnv).


Uploading the data to Amazon Web-services: S3 requires:
$ sudo apt-get install awscli
and the user has to be configured using security credentials generated in the AWS console

$ gem install aws-sdk

ENVIRONMENT VARIABLES:
--catalogue for files
UPLOAD_PATH
--postgresql db-info
DB_NAME
DB_USER
DB_PW
--credentials and stuff for AWS
AWS_SDK_CONFIG_OPT_OUT=1 (TRUE)
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_REGION
--bucketname
S3_BUCKET

Suggestion for crontab entry if you want to run the script on a monthly basis,
middle of month because we don't want to deal with the 28-31 days issue.

0 4 15 * * ./script.rb

#!/usr/bin/ruby
include this if you want the script to execute without calling the ruby interpreter

=end
require 'rubygems'
require 'bundler/setup'
Bundler.require(:default)

begin
  time = Time.new
  $stdout.reopen("#{ENV['UPLOAD_PATH']}view-buy.csv", "w")
  $stderr.reopen("#{ENV['UPLOAD_PATH']}#{time.month}-#{time.year}-err.txt", "w")

  #puts('invoice_id,article_number')

  con = PG.connect :dbname => ENV['DB_NAME'],
                   :user => ENV['DB_USER'],
                   :password => ENV['DB_PW']

  rs = con.exec 'SELECT sql-setning'
 
  rs.each do |row|
    ctime = DateTime.parse(row['timestamp'])
    puts '%s::%s::%s' % [ row['user_id'], row['article_no'], ctime.to_time.to_i ]
  end

  
  s3 = Aws::S3::Resource.new
  obj = s3.bucket(ENV['S3_BUCKET']).object("#{time.month}-#{time.year}.csv")
  obj.upload_file("#{ENV['UPLOAD_PATH']}#{time.month}-#{time.year}.csv")

rescue PG::Error => e
  warn e.message

ensure
  rs.clear if rs
  con.close if con

end
