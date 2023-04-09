require "database_cleaner/generic/truncation"
require 'database_cleaner/data_mapper/base'
require "dm-types"
# require 'data_mapper'

module DataMapper
  module Adapters

    class DataObjectsAdapter

      def storage_names(repository = :default)
        raise NotImplementedError
      end

      def truncate_tables(table_names)
        table_names.each do |table_name|
          truncate_table table_name
        end
      end

    end

    class MysqlAdapter < DataObjectsAdapter

      # taken from https://github.com/godfat/dm-mapping/tree/master
      def storage_names(repository = :default)
        select 'SHOW TABLES'
      end

      def truncate_table(table_name)
        execute("TRUNCATE TABLE #{quote_name(table_name)};")
      end

      # copied from activerecord
      def disable_referential_integrity
        old = select("SELECT @@FOREIGN_KEY_CHECKS;")
        begin
          execute("SET FOREIGN_KEY_CHECKS = 0;")
          yield
        ensure
          execute("SET FOREIGN_KEY_CHECKS = ?", *old)
        end
      end

    end

    # FIXME
    # i don't know if this works
    # i basically just copied activerecord code to get a rough idea what they do.
    # i don't have postgres available, so i won't be the one to write this.
    # maybe codes below gets some postgres/datamapper user going, though.
    class PostgresAdapter < DataObjectsAdapter

      # taken from https://github.com/godfat/dm-mapping/tree/master
      def storage_names(repository = :default)
        sql = <<-SQL
          SELECT table_name FROM "information_schema"."tables"
          WHERE table_schema = current_schema() and table_type = 'BASE TABLE'
        SQL
        select(sql)
      end

      def truncate_table(table_name)
        execute("TRUNCATE TABLE #{quote_name(table_name)} RESTART IDENTITY CASCADE;")
      end

      # override to use a single statement
      def truncate_tables(table_names)
        quoted_names = table_names.collect { |n| quote_name(n) }.join(', ')
        execute("TRUNCATE TABLE #{quoted_names} RESTART IDENTITY;")
      end

      # FIXME
      # copied from activerecord
      def supports_disable_referential_integrity?
        version = select("SHOW server_version")[0][0].split('.')
        (version[0].to_i >= 8 && version[1].to_i >= 1) ? true : false
      rescue
        return false
      end

      # FIXME
      # copied unchanged from activerecord
      def disable_referential_integrity(repository = :default)
        if supports_disable_referential_integrity? then
          execute(storage_names(repository).collect do |name|
            "ALTER TABLE #{quote_name(name)} DISABLE TRIGGER ALL"
          end.join(";"))
        end
        yield
      ensure
        if supports_disable_referential_integrity? then
          execute(storage_names(repository).collect do |name|
            "ALTER TABLE #{quote_name(name)} ENABLE TRIGGER ALL"
          end.join(";"))
        end
      end

    end

  end
end


module DatabaseCleaner
  module DataMapper
    class Truncation
      include ::DatabaseCleaner::DataMapper::Base
      include ::DatabaseCleaner::Generic::Truncation

      def clean(repository = self.db)
        adapter = ::DataMapper.repository(repository).adapter
        adapter.disable_referential_integrity do
          adapter.truncate_tables(tables_to_truncate(repository))
        end
      end

      private

      def tables_to_truncate(repository = self.db)
        (@only || ::DataMapper.repository(repository).adapter.storage_names(repository)) - @tables_to_exclude
      end

      # overwritten
      def migration_storage_names
        %w[migration_info]
      end

    end
  end
end
