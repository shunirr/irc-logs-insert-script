#!/usr/bin/env ruby

require 'mysql2-cs-bind'
require 'pit'

def get_channel_id(client, name)
  channel_id = nil
  client.xquery("SELECT id, name FROM channel WHERE name=?", name).each do |data|
    channel_id = data['id']
  end
  unless channel_id
    client.xquery("INSERT INTO channel (name,created_on,updated_on,readed_on) VALUES (?,?,?,?)", name, Time.now, Time.now, Time.now)
    client.xquery("SELECT id, name FROM channel WHERE name=?", name).each do |data|
      channel_id = data['id']
    end
  end
  channel_id
end

def get_nick_id(client, name)
  nick_id = nil
  client.xquery("SELECT id, name FROM nick WHERE name=?", name).each do |data|
    nick_id = data['id']
  end
  unless nick_id
    client.xquery("INSERT INTO nick (name,created_on,updated_on) VALUES (?,?,?)", name, Time.now, Time.now)
    client.xquery("SELECT id, name FROM nick WHERE name=?", name).each do |data|
      nick_id = data['id']
    end
  end
  nick_id
end

log_dir = nil
if ARGV[0]
  log_dir = ARGV[0]
else
  puts "#{$0} LOGS_DIR"
  exit 0
end

config = Pit.get('tiarra', :require => {
    "host"     => "MYSQL_HOST",
    "username" => "MYSQL_USERNAME",
    "password" => "MYSQL_PASSWORD",
    "database" => "MYSQL_DATABASE",
})

client = Mysql2::Client.new(host:     config['host'],
                            username: config['username'],
                            password: config['password'],
                            database: config['database'])

Dir.glob("#{log_dir}/*.log") do |file|
  user, network, o = File.basename(file, '.log').scan(/([^_]+?)_([^_]+?)_(.*)$/).flatten
  channel, date = o.scan(/(.*)_(\d+)/).flatten
  year, month, day = date.scan(/(\d\d\d\d)(\d\d)(\d\d)/).flatten

  # tiarra っぽいチャンネル名にする
  channel = "#{channel}@#{network}"
  channel_id = get_channel_id(client, channel)

  logs = open(file).read
  logs.encode("UTF-16BE", "UTF-8", invalid: :replace, undef: :replace, replace: '.')
  logs.encode("UTF-8")
  logs.split(/(\n|\r|\r\n)/).each do |line|
    if line =~ /^\[(\d+):(\d+):(\d+)\] <([^>]*)> (.*)$/
      notice = false
    elsif line =~ /^\[(\d+):(\d+):(\d+)\] -(.*)- (.*)$/
      notice = true
    else
      next
    end
    time = Time.local year.to_i, month.to_i, day.to_i, $1.to_i, $2.to_i, $3.to_i
    nick = $4
    text = $5
    nick_id = get_nick_id(client, nick)
    is_notice = (notice) ? '1' : '0'

    p line
    client.xquery("INSERT INTO log (channel_id,nick_id,log,is_notice,created_on,updated_on) VALUES (?,?,?,?,?,?)", channel_id, nick_id, text, is_notice, time, time)
  end
end
