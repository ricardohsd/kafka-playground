require 'avro'

module AvroSerde
  # Patches AvroTurf::SchemaStore in order to have a find method that return
  # the correct schema content
  # https://github.com/dasch/avro_turf/blob/master/lib/avro_turf/schema_store.rb
  class SchemaStore
    def initialize(path: nil)
      @path = path or raise "Please specify a schema path"
      @avro = Hash.new
      @raw_schemas = Hash.new
    end

    # Resolves and returns a schema.
    #
    # schema_name - The String name of the schema to resolve.
    #
    # Returns an Avro::Schema and a string with its raw schema.
    def find(name, namespace = nil)
      fullname = Avro::Name.make_fullname(name, namespace)

      return [@avro[fullname], @raw_schemas[fullname]] if @raw_schemas.key?(fullname)

      *namespace, schema_name = fullname.split(".")
      schema_path = File.join(@path, *namespace, schema_name + ".avsc")
      schema_json = JSON.parse(File.read(schema_path))
      schema = Avro::Schema.real_parse(schema_json, @avro)

      raw_schema = JSON.dump(schema_json)
      @raw_schemas[fullname] = raw_schema

      if schema.respond_to?(:fullname) && schema.fullname != fullname
        raise AvroTurf::SchemaError, "expected schema `#{schema_path}' to define type `#{fullname}'"
      end

      # returns avro object and the original schema content
      [schema, raw_schema]
    rescue ::Avro::SchemaParseError => e
      # This is a hack in order to figure out exactly which type was missing. The
      # Avro gem ought to provide this data directly.
      if e.to_s =~ /"([\w\.]+)" is not a schema we know about/
        find($1)

        # Re-resolve the original schema now that the dependency has been resolved.
        @schemas.delete(fullname)
        find(fullname)
      else
        raise
      end
    rescue Errno::ENOENT, Errno::ENAMETOOLONG
      raise AvroTurf::SchemaNotFoundError, "could not find Avro schema at `#{schema_path}'"
    end

    # Loads all schema definition files in the `schemas_dir`.
    def load_schemas!
      pattern = [@path, "**", "*.avsc"].join("/")

      Dir.glob(pattern) do |schema_path|
        # Remove the path prefix.
        schema_path.sub!(/^\/?#{@path}\//, "")

        # Replace `/` with `.` and chop off the file extension.
        schema_name = File.basename(schema_path.tr("/", "."), ".avsc")

        # Load and cache the schema.
        find(schema_name)
      end
    end
  end
end
