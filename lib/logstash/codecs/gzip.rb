# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"
require "zlib"
require "stringio"

# This codec may be used to decode gzip data from string. 
#
# If this codec recieves a payload from an input that is not valid GZIP, then
# it will fall back to plain text and add a tag `_gzipparsefailure`. Upon a GZIP
# failure, the payload will be stored in the `message` field.
class LogStash::Codecs::GZIP < LogStash::Codecs::Base
  config_name "gzip"

  # The character encoding used in this codec. Examples include "UTF-8" and
  # "CP1252".
  #
  # GZIP requires valid UTF-8 strings, but in some cases, software that
  # emits GZIP does so in another encoding (nxlog, for example). In
  # weird cases like this, you can set the `charset` setting to the
  # actual encoding of the text and Logstash will convert it for you.
  #
  # For nxlog users, you may to set this to "CP1252".
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  public
  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  public
  def decode(data)
    begin
      decoded = Zlib::GzipReader.new(StringIO.new(data)).read
      yield LogStash::Event.new("message" => @converter.convert(decoded))
    rescue Zlib::Error, Zlib::GzipFile::Error=> e
	  @logger.info? && @logger.info("GZIP parse failure. Falling back to plain-text", :error => e, :data => data)
      yield LogStash::Event.new("message" => data, "tags" => ["_gzipparsefailure"])
    end
  end # def decode
end # class LogStash::Codecs::JSON
