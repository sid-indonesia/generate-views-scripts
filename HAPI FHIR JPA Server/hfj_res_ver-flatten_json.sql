CREATE SCHEMA IF NOT EXISTS generated_views;
-- Generalized solution (for jsonb) https://stackoverflow.com/a/35179515
-- Create function "Create HAPI FHIR `hfj_res_ver` grouped by `res_type` and `res_text_vc` type changed to `jsonb` view"
CREATE OR REPLACE FUNCTION generated_views.create_json_view (
        table_name TEXT,
        regular_columns TEXT,
        json_column TEXT,
        identifier_value TEXT,
        identifier_column TEXT
    ) RETURNS TEXT LANGUAGE plpgsql AS $$ BEGIN EXECUTE format(
        $ex$ DROP VIEW IF EXISTS generated_views."%4$s_all_versions_view";
CREATE VIEW generated_views."%4$s_all_versions_view" AS
SELECT %2$s,
    %3$s::jsonb AS "%4$s"
FROM %1$s e
WHERE e.%5$s = %4$L $ex$,
    table_name,
    regular_columns,
    json_column,
    identifier_value,
    identifier_column
);
RETURN 1;
END $$;
-- 
-- HAPI FHIR hfj_res_ver generate all FHIR resources views
CREATE OR REPLACE FUNCTION generated_views.generate_all_fhir_resources_views() RETURNS TEXT LANGUAGE plpgsql AS $$
DECLARE the_queries TEXT;
BEGIN EXECUTE format (
    $ex$
    SELECT string_agg(
            format(
                'generated_views.create_json_view(''public.hfj_res_ver'',''pid, partition_date, partition_id, res_deleted_at, res_version, has_tags, res_published, res_updated, res_encoding, res_text, res_id, res_type, res_ver'',''res_text_vc'',%%1$L,''res_type'')',
                "res_type_value"
            ),
            ', '
        )
    FROM (
            SELECT DISTINCT all_fhir_resources_all_versions.res_type AS "res_type_value"
            FROM public."hfj_res_ver" all_fhir_resources_all_versions
            ORDER BY 1
        ) s;
$ex$
) INTO the_queries;
EXECUTE format(
    $ex$
    SELECT %1$s $ex$,
        the_queries
);
RETURN the_queries;
END $$;
SELECT generated_views.generate_all_fhir_resources_views();
