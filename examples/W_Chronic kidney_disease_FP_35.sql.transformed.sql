/*  Translated SQL for Enclave */

/* Referenced Tables: ['codesets'] */
CREATE TABLE codesets
 AS
SELECT
cast(NULL AS int) AS codeset_id,
	cast(NULL AS bigint) AS concept_id  WHERE 1 = 0;

/* Referenced Tables: ['concept', 'concept_ancestor', 'codesets'] */
WITH insertion_temp AS (
(SELECT 0 AS codeset_id, c.concept_id FROM (SELECT DISTINCT i.concept_id FROM
(
 SELECT concept_id FROM concept WHERE concept_id IN (194385,46271022,192279,4263367,261071,201313,4103224,193253,195314,192359,45768812)
UNION SELECT c.concept_id
 FROM concept c
 JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
 AND ca.ancestor_concept_id IN (194385,46271022,192279,4263367,261071,201313,4103224,195314,192359,45768812)
 AND c.invalid_reason IS NULL
) i
LEFT JOIN
(
 SELECT concept_id FROM concept WHERE concept_id IN (45769152,195289,195737,43530912,4066005,37116834,195014,197930,197320)
UNION SELECT c.concept_id
 FROM concept c
 JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
 AND ca.ancestor_concept_id IN (45769152,195289,195737,43530912,4066005,37116834,195014,197930,197320)
 AND c.invalid_reason IS NULL
) e ON i.concept_id = e.concept_id
WHERE e.concept_id IS NULL
) c UNION ALL
SELECT 1 AS codeset_id, c.concept_id FROM (SELECT DISTINCT i.concept_id FROM
(
 SELECT concept_id FROM concept WHERE concept_id IN (4090651,4032243,45889365,4027133,38003431)
UNION SELECT c.concept_id
 FROM concept c
 JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
 AND ca.ancestor_concept_id IN (4090651,4032243,45889365,4027133,38003431)
 AND c.invalid_reason IS NULL
) i
) c
) UNION ALL (SELECT codeset_id, concept_id FROM codesets ))
SELECT * FROM insertion_temp;

/* Referenced Tables: ['qualified_events', 'condition_occurrence', 'codesets', 'observation_period', 'procedure_occurrence', 'observation'] */
CREATE TABLE qualified_events
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
 SELECT pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() OVER (PARTITION BY pe.person_id ORDER BY pe.start_date ASC) AS ordinal, cast(pe.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) pe
JOIN (
-- begin criteria group
SELECT 0 AS index_id, person_id, event_id
FROM
(
 SELECT e.person_id, e.event_id
 FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) e
 INNER JOIN
 (
 -- begin correlated criteria
SELECT 0 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) p
JOIN (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, 30) AND a.start_date <= p.op_end_date ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 1
-- end correlated criteria
UNION ALL
-- begin correlated criteria
SELECT 1 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) p
JOIN (
 -- begin procedure occurrence criteria
SELECT c.person_id, c.procedure_occurrence_id AS event_id, c.procedure_date AS start_date, date_add(c.procedure_date, 1) AS end_date,
 c.visit_occurrence_id, c.procedure_date AS sort_date
FROM
(
 SELECT po.*
 FROM procedure_occurrence po
JOIN codesets cs ON (po.procedure_concept_id = cs.concept_id AND cs.codeset_id = 1)
) c
-- end procedure occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, 0) AND a.start_date <= p.op_end_date ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 2
-- end correlated criteria
UNION ALL
-- begin correlated criteria
SELECT 2 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) p
JOIN (
 -- begin observation criteria
SELECT c.person_id, c.observation_id AS event_id, c.observation_date AS start_date, date_add(c.observation_date, 1) AS end_date,
 c.visit_occurrence_id, c.observation_date AS sort_date
FROM
(
 SELECT o.*
 FROM observation o
JOIN codesets cs ON (o.observation_concept_id = cs.concept_id AND cs.codeset_id = 1)
) c
-- end observation criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, 0) AND a.start_date <= p.op_end_date ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 2
-- end correlated criteria
UNION ALL
-- begin correlated criteria
SELECT 3 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (-- begin primary events
SELECT p.ordinal AS event_id, p.person_id, p.start_date, p.end_date, op_start_date, op_end_date, cast(p.visit_occurrence_id AS bigint) AS visit_occurrence_id
FROM
(
 SELECT e.person_id, e.start_date, e.end_date,
 row_number() OVER (PARTITION BY e.person_id ORDER BY e.sort_date ASC, e.event_id) ordinal,
 op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date, cast(e.visit_occurrence_id AS bigint) AS visit_occurrence_id
 FROM
 (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 0)
) c
-- end condition occurrence criteria
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) p
JOIN (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 1)
) c
-- end condition occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, 0) AND a.start_date <= p.op_end_date ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 2
-- end correlated criteria
 ) cq ON e.person_id = cq.person_id AND e.event_id = cq.event_id
 GROUP BY e.person_id, e.event_id
 HAVING count(index_id) > 0
) g
-- end criteria group
) ac ON ac.person_id = pe.person_id AND ac.event_id = pe.event_id
) qe
WHERE qe.ordinal = 1
;

/* Referenced Tables: ['inclusion_events'] */
CREATE TABLE inclusion_events
 AS
SELECT
cast(NULL AS bigint) AS inclusion_rule_id,
	cast(NULL AS bigint) AS person_id,
	cast(NULL AS bigint) AS event_id  WHERE 1 = 0;

/* Referenced Tables: ['included_events', 'qualified_events', 'inclusion_events'] */
CREATE TABLE included_events
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() OVER (PARTITION BY person_id ORDER BY start_date ASC) AS ordinal
 FROM
 (
 SELECT q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date, sum(coalesce(power(cast(2 AS bigint), i.inclusion_rule_id), 0)) AS inclusion_rule_mask
 FROM qualified_events q
 LEFT JOIN inclusion_events i ON i.person_id = q.person_id AND i.event_id = q.event_id
 GROUP BY q.event_id, q.person_id, q.start_date, q.end_date, q.op_start_date, q.op_end_date
 ) mg -- matching groups
) results
WHERE results.ordinal = 1
;

/* Referenced Tables: ['cohort_rows', 'included_events'] */
CREATE TABLE cohort_rows
AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
 SELECT f.person_id, f.start_date, f.end_date
 FROM (
 SELECT i.event_id, i.person_id, i.start_date, ce.end_date, row_number() OVER (PARTITION BY i.person_id, i.event_id ORDER BY ce.end_date) AS ordinal
 FROM included_events i
 JOIN ( -- cohort_ends
-- cohort exit dates
-- by default, cohort exit at the event's op end date
SELECT event_id, person_id, op_end_date AS end_date FROM included_events
 ) ce ON i.event_id = ce.event_id AND i.person_id = ce.person_id AND ce.end_date >= i.start_date
 ) f
 WHERE f.ordinal = 1
) fe;

/* Referenced Tables: ['final_cohort', 'cohort_rows'] */
CREATE TABLE final_cohort
AS
SELECT
person_id, min(start_date) AS start_date, end_date
FROM
( --cteends
 SELECT
 c.person_id
 , c.start_date
 , min(ed.end_date) AS end_date
 FROM cohort_rows c
 JOIN ( -- cteenddates
 SELECT
 person_id
 , date_add(event_date, -1 * 0) AS end_date
 FROM
 (
 SELECT
 person_id
 , event_date
 , event_type
 , sum(event_type) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED preceding) AS interval_status
 FROM
 (
 SELECT
 person_id
 , start_date AS event_date
 , -1 AS event_type
 FROM cohort_rows
 UNION ALL
 SELECT
 person_id
 , date_add(end_date, 0) AS end_date
 , 1 AS event_type
 FROM cohort_rows
 ) rawdata
 ) e
 WHERE interval_status = 0
 ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
 GROUP BY c.person_id, c.start_date
) e
GROUP BY person_id, end_date
;

/* Referenced Tables: [] */
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  SELECT * FROM @target_database_schema.@target_cohort_table  WHERE NOT (cohort_definition_id = @target_cohort_id);

/* Referenced Tables: ['final_cohort'] */
WITH insertion_temp AS (
(SELECT @target_cohort_id AS cohort_definition_id, person_id, start_date, end_date
FROM final_cohort co
) UNION ALL (SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM @target_database_schema.@target_cohort_table ))
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT * FROM insertion_temp;

/* Referenced Tables: ['cohort_rows'] */
TRUNCATE TABLE cohort_rows;

/* Referenced Tables: ['cohort_rows'] */
DROP TABLE cohort_rows;

/* Referenced Tables: ['final_cohort'] */
TRUNCATE TABLE final_cohort;

/* Referenced Tables: ['final_cohort'] */
DROP TABLE final_cohort;

/* Referenced Tables: ['inclusion_events'] */
TRUNCATE TABLE inclusion_events;

/* Referenced Tables: ['inclusion_events'] */
DROP TABLE inclusion_events;

/* Referenced Tables: ['qualified_events'] */
TRUNCATE TABLE qualified_events;

/* Referenced Tables: ['qualified_events'] */
DROP TABLE qualified_events;

/* Referenced Tables: ['included_events'] */
TRUNCATE TABLE included_events;

/* Referenced Tables: ['included_events'] */
DROP TABLE included_events;

/* Referenced Tables: ['codesets'] */
TRUNCATE TABLE codesets;

/* Referenced Tables: ['codesets'] */
DROP TABLE codesets;

