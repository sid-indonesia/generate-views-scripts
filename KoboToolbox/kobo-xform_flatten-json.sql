-- Generalized solution (for jsonb) https://stackoverflow.com/a/35179515
-- Create function "Create Kobo flattened logger instace view"
CREATE
OR REPLACE FUNCTION generated_views.create_logger_instance_json_flat_view (
    table_name TEXT,
    regular_columns TEXT,
    json_column TEXT,
    xform_id INT4,
    identifier_column TEXT
) RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE cols TEXT;

BEGIN EXECUTE format (
    $ex$
    SELECT
        string_agg(format('%2$s :: jsonb ->>%%1$L "%%2s_%%1$s"', KEY, row_number), ', ')
    FROM
        (
            SELECT KEY,
            row_number() OVER (ORDER BY KEY) as row_number
            FROM 
                (
                    SELECT
                        DISTINCT KEY
                    FROM
                        %1$s li,
                        jsonb_each(%2$s :: jsonb)
                    WHERE
                        li.%4$s = %3$L
                    ORDER BY
                        1
                ) s
        ) t;

$ex$,
table_name,
json_column,
xform_id,
identifier_column
) INTO cols;

EXECUTE format(
    $ex$ DROP VIEW IF EXISTS generated_views."%1$s_%6$s_%5$s_view";

CREATE VIEW generated_views."%1$s_%6$s_%5$s_view" AS
SELECT
    %2$s,
    %3$s
FROM
    %1$s e
WHERE
    e.%6$s = %5$L $ex$,
    table_name,
    regular_columns,
    cols,
    json_column,
    xform_id,
    identifier_column
);

RETURN cols;

END $$;



-- Kobo logger_instace generate all xform views
CREATE
OR REPLACE FUNCTION generated_views.generate_all_xform_views() RETURNS TEXT LANGUAGE plpgsql AS $$ DECLARE the_queries TEXT;

BEGIN EXECUTE format (
    $ex$
    SELECT
        string_agg(
            format(
                'generated_views.create_logger_instance_json_flat_view(''public.logger_instance'',''id, xml, date_created, date_modified, deleted_at, status, uuid, geom, survey_type_id, user_id, xform_id, xml_hash, is_synced_with_mongo, posted_to_kpi'',''json'',%%1$L,''xform_id'')',
                "xform_id"
            ),
            ', '
        )
    FROM
        (
            SELECT
                DISTINCT li.xform_id AS "xform_id"
            FROM
                public."logger_instance" li
            ORDER BY
                1
        ) s;

$ex$
) INTO the_queries;

EXECUTE format(
    $ex$
    SELECT
        %1$s $ex$,
        the_queries
);

RETURN the_queries;

END $$;

SELECT
    generated_views.generate_all_xform_views();
