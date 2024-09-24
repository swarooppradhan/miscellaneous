WITH expanded_data AS (
    SELECT
        json_extract(raw, '$.issues') AS issues_array
    FROM your_table_name
)
SELECT
    json_extract_scalar(issue.value, '$.id') AS issue_id,
    json_extract_scalar(issue.value, '$.key') AS issue_key,
    json_extract_scalar(issue.value, '$.fields.summary') AS summary,
    json_extract_scalar(issue.value, '$.fields.status.name') AS status,
    json_extract_scalar(issue.value, '$.fields.priority.name') AS priority,
    json_extract_scalar(issue.value, '$.fields.assignee.displayName') AS assignee,
    json_extract_scalar(issue.value, '$.fields.created') AS created_date
FROM expanded_data
CROSS JOIN UNNEST(cast(json_parse(issues_array) AS array(json))) AS t(issue);
