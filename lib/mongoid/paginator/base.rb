module Mongoid
  module Paginator
    class Base
      def initialize(cursor, per_page, offset, sort_by, order, fields)
        @per_page = per_page        # Results per page (eg 30)
        @offset = offset            # params[:offset] - either "#{next_page_first_doc.id}" or "#{next_page_first_doc.send(:sort_by)}-#{next_page_first_doc.id}"
        @sort_by = sort_by          # params[:sort_by] - field to sort by, eg "occurrences"
        @order = order              # params[:order] - direction of the sort (asc or desc)
        @fields = fields

        # Sorted cursor
        @cursor = cursor.order_by(sort_field => @order, id: @order)

        decode_offset
      end

      def offset_provided?
        @id_offset || @field_offset
      end

      def results
        results_plus_one[0...@per_page]
      end

      def has_next?
        results_plus_one.length == @per_page + 1
      end

      def has_prev?
        offset_provided?
      end

      def next_offset
        @next_offset ||= encode_offset(results.last)
      end

      def prev_offset
        return nil unless offset_provided?

        offset_doc = prev_cursor.only(sort_field, :id).offset(@per_page).limit(1).first
        return nil if offset_doc.nil?

        encode_offset(offset_doc)
      end

      def start
        @start ||= (has_prev? ? prev_cursor.count : 0) + 1
      end

      def count
        @count ||= results.count
      end

      def total
        @total ||= @cursor.count
      end

      def position
        total < @per_page ? "#{total} of #{total}" : "#{start} - #{start + count - 1} of #{total}"
      end


      protected
      def decode_offset
        return unless @offset.present?

        offset_parts = @offset.split("-")
        @id_offset = offset_parts.first

        if offset_parts.length == 2
          # Decode the field_offset based on the type
          @field_offset = case sort_field_type
            when Time, Moped::BSON::ObjectId
              Time.at(offset_parts.last.to_i).utc
            else
              offset_parts.last
            end
        end
      end

      def encode_offset(doc)
        return doc.id.to_s if sort_field_type == Moped::BSON::ObjectId

        value = if sort_field.to_s.include?(".")
            field, key = sort_field.split(".")
            doc.try(field).try(:[], key)
          else
            doc.send(sort_field)
          end

        field_offset = case value
          when Moped::BSON::ObjectId
            value.generation_time.to_i
          when Time
            value.to_i
          else
            value.to_s
          end

        "#{doc.id}-#{field_offset}"
      end

      def sort_field
        field_info = @fields[@sort_by.to_sym]
        return "_id" if field_info.nil?

        f = field_info[:field] == :id ? :_id : field_info[:field]
        f.to_s
      end

      def sort_field_type
        # Look up field type from provided sort fields hash
        field_info = @fields[@sort_by.to_sym]
        return field_info[:type] if field_info && field_info[:type]

        # Try to automatically determine the field type
        field_model_info = @cursor.klass.send(:fields)[sort_field]
        field_model_info.options[:type] if field_model_info
      end

      def results_cursor
        @results_cursor ||= scope_cursor(@cursor, false, @id_offset, {sort_field => @field_offset})
      end

      def prev_cursor
        unless @prev_cursor
          cursor = invert_sorts(@cursor)
          cursor = scope_cursor(cursor, true, @id_offset, {sort_field => @field_offset})

          @prev_cursor = cursor
        end

        @prev_cursor
      end

      def results_plus_one
        @results_plus_one ||= results_cursor.limit(@per_page + 1).to_a
      end

      def scope_cursor(criteria, inclusive=false, id_offset=nil, field_offsets={})
        return criteria if id_offset.nil?

        selectors = []
        offset_chain = field_offsets.merge({"_id" => id_offset}).to_a
        # raise offset_chain.inspect
        while !offset_chain.blank?
          idx = 0
          selector = offset_chain.each_with_object({}) do |(field, offset), h|
            if idx == offset_chain.length - 1
              comp = comparator(criteria, field, inclusive)
              h[field] = {comp => offset} unless comp.nil? || offset.nil?
            else
              h[field] = offset
            end

            idx += 1
          end
          selectors << selector unless selector.blank?

          offset_chain.pop
        end

        selectors << {field_offsets.to_a.first.first => nil} unless field_offsets.empty?

        # TODO: Fix for null fields
        # [{"app.releaseStage"=>"development", "_id"=>{"$lt"=>"52a26631bc3b18cad800003f"}}, {"app.releaseStage"=>{"$lt"=>"development"}}]

        criteria.and(selectors.length > 1 ? {"$or" => selectors} : selectors.first)
      end

      def comparator(criteria, field, inclusive=false)
        field_sort = sort_options(criteria)[field]
        return nil if field_sort.nil?

        comparator = sort_options(criteria)[field] == 1 ? "$gt" : "$lt"
        inclusive ? "#{comparator}e" : comparator
      end

      def sort_options(criteria)
        criteria.options[:sort] || {"_id" => 1}
      end

      def invert_sorts(criteria)
        criteria.order_by(Hash[sort_options(criteria).map{|k, v| [k, -1*v]}])
      end
    end
  end
end