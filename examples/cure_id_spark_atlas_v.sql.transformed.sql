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
 SELECT concept_id FROM concept WHERE concept_id IN (260125,254058,260139,4112521,4110483,4110485,4110023,46273719,46271075,4307774,4051332,36716978,37016114,4112359,4195694,319049,4110484,4112015,257011,312940,254677,442555,4059022,4059021,40482069,256451,256722,4059003,4168213,434490,4140438,314971,439676,254761,4048098,37311061,4100065,320136,4038519,312437,4060052,4263848,37311059,37016200,4011766,437663,4141062,4164645,4047610,46271074,4260205,4310964,37395564,4133224,4185711,4289517,4140453,4090569,439857,4109381,4330445,255848,40482061,436145,40479642,4102774,4256228,320651,436235,261326,3661405,3661406,3662381,756031,37311061,3663281,3661408,756039,320651)
) i
) c UNION ALL
SELECT 4 AS codeset_id, c.concept_id FROM (SELECT DISTINCT i.concept_id FROM
(
 SELECT concept_id FROM concept WHERE concept_id IN (9201)
) i
) c UNION ALL
SELECT 5 AS codeset_id, c.concept_id FROM (SELECT DISTINCT i.concept_id FROM
(
 SELECT concept_id FROM concept WHERE concept_id IN (260139,46271075,4307774,4195694,257011,442555,4059022,4059021,256451,4059003,4168213,434490,439676,254761,4048098,37311061,4100065,320136,4038519,312437,4060052,4263848,37311059,37016200,4011766,437663,4141062,4164645,4047610,4260205,4185711,4289517,4140453,4090569,4109381,4330445,255848,4102774,320651,436235,261326,3661406,3662381,756031,3661408,756039,3661405,37311061,37310284,37310283,37310286,3663281,37310287,37310254,4193169,260125)
UNION SELECT c.concept_id
 FROM concept c
 JOIN concept_ancestor ca ON c.concept_id = ca.descendant_concept_id
 AND ca.ancestor_concept_id IN (3661406,3662381,756031,3661408,756039,3661405,37311061,37310284,37310283,37310286,3663281,37310287,37310254)
 AND c.invalid_reason IS NULL
) i
) c UNION ALL
SELECT 6 AS codeset_id, c.concept_id FROM (SELECT DISTINCT i.concept_id FROM
(
 SELECT concept_id FROM concept WHERE concept_id IN (36661370,706167,706157,706155,36661371,715272,757678,706161,586524,586525,36661378,36032258,586520,706175,706156,706154,723469,706168,723478,36031506,723464,723471,723470,36031652,706160,36032174,706173,36031453,586528,586529,715262,723476,586526,757677,36031238,706163,36661377,715260,715261,723463,706170,706158,36032061,706169,723467,723468,723465,36031213,586519,723466,36031944,586517)
) i
) c
) UNION ALL (SELECT codeset_id, concept_id FROM codesets ))
SELECT * FROM insertion_temp;

/* Referenced Tables: ['qualified_events', 'measurement', 'codesets', 'observation_period', 'condition_occurrence', 'visit_occurrence', 'observation_period'] */
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
 SELECT pe.person_id, pe.event_id, pe.start_date, pe.end_date, pe.visit_occurrence_id, pe.sort_date FROM (
-- begin measurement criteria
SELECT c.person_id, c.measurement_id AS event_id, c.measurement_date AS start_date, date_add(c.measurement_date, 1) AS end_date,
 c.visit_occurrence_id, c.measurement_date AS sort_date
FROM
(
 SELECT m.*
 FROM measurement m
JOIN codesets cs ON (m.measurement_concept_id = cs.concept_id AND cs.codeset_id = 6)
) c
WHERE c.measurement_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
AND c.value_as_concept_id IN (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- end measurement criteria
) pe
JOIN (
-- begin criteria group
SELECT 0 AS index_id, person_id, event_id
FROM
(
 SELECT e.person_id, e.event_id
 FROM (SELECT q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date
FROM (-- begin measurement criteria
SELECT c.person_id, c.measurement_id AS event_id, c.measurement_date AS start_date, date_add(c.measurement_date, 1) AS end_date,
 c.visit_occurrence_id, c.measurement_date AS sort_date
FROM
(
 SELECT m.*
 FROM measurement m
JOIN codesets cs ON (m.measurement_concept_id = cs.concept_id AND cs.codeset_id = 6)
) c
WHERE c.measurement_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
AND c.value_as_concept_id IN (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- end measurement criteria
) q
JOIN observation_period op ON q.person_id = op.person_id
 AND op.observation_period_start_date <= q.start_date AND op.observation_period_end_date >= q.start_date
) e
 INNER JOIN
 (
 -- begin correlated criteria
SELECT 0 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (SELECT q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date
FROM (-- begin measurement criteria
SELECT c.person_id, c.measurement_id AS event_id, c.measurement_date AS start_date, date_add(c.measurement_date, 1) AS end_date,
 c.visit_occurrence_id, c.measurement_date AS sort_date
FROM
(
 SELECT m.*
 FROM measurement m
JOIN codesets cs ON (m.measurement_concept_id = cs.concept_id AND cs.codeset_id = 6)
) c
WHERE c.measurement_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
AND c.value_as_concept_id IN (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- end measurement criteria
) q
JOIN observation_period op ON q.person_id = op.person_id
 AND op.observation_period_start_date <= q.start_date AND op.observation_period_end_date >= q.start_date
) p
JOIN (
 -- begin condition occurrence criteria
SELECT c.person_id, c.condition_occurrence_id AS event_id, c.condition_start_date AS start_date, coalesce(c.condition_end_date, date_add(c.condition_start_date, 1)) AS end_date,
 c.visit_occurrence_id, c.condition_start_date AS sort_date
FROM
(
 SELECT co.*
 FROM condition_occurrence co
 JOIN codesets cs ON (co.condition_concept_id = cs.concept_id AND cs.codeset_id = 5)
) c
WHERE c.condition_start_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
-- end condition occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, -21) AND a.start_date <= date_add(p.start_date, 34) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 1
-- end correlated criteria
UNION ALL
-- begin correlated criteria
SELECT 1 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (SELECT q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date
FROM (-- begin measurement criteria
SELECT c.person_id, c.measurement_id AS event_id, c.measurement_date AS start_date, date_add(c.measurement_date, 1) AS end_date,
 c.visit_occurrence_id, c.measurement_date AS sort_date
FROM
(
 SELECT m.*
 FROM measurement m
JOIN codesets cs ON (m.measurement_concept_id = cs.concept_id AND cs.codeset_id = 6)
) c
WHERE c.measurement_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
AND c.value_as_concept_id IN (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- end measurement criteria
) q
JOIN observation_period op ON q.person_id = op.person_id
 AND op.observation_period_start_date <= q.start_date AND op.observation_period_end_date >= q.start_date
) p
JOIN (
 -- begin visit occurrence criteria
SELECT c.person_id, c.visit_occurrence_id AS event_id, c.visit_start_date AS start_date, c.visit_end_date AS end_date,
 c.visit_occurrence_id, c.visit_start_date AS sort_date
FROM
(
 SELECT vo.*
 FROM visit_occurrence vo
JOIN codesets cs ON (vo.visit_concept_id = cs.concept_id AND cs.codeset_id = 4)
) c
-- end visit occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, -7) AND a.start_date <= date_add(p.start_date, 21) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 1
-- end correlated criteria
UNION ALL
-- begin correlated criteria
SELECT 2 AS index_id, cc.person_id, cc.event_id
FROM (SELECT p.person_id, p.event_id
FROM (SELECT q.person_id, q.event_id, q.start_date, q.end_date, q.visit_occurrence_id, op.observation_period_start_date AS op_start_date, op.observation_period_end_date AS op_end_date
FROM (-- begin measurement criteria
SELECT c.person_id, c.measurement_id AS event_id, c.measurement_date AS start_date, date_add(c.measurement_date, 1) AS end_date,
 c.visit_occurrence_id, c.measurement_date AS sort_date
FROM
(
 SELECT m.*
 FROM measurement m
JOIN codesets cs ON (m.measurement_concept_id = cs.concept_id AND cs.codeset_id = 6)
) c
WHERE c.measurement_date >= to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
AND c.value_as_concept_id IN (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- end measurement criteria
) q
JOIN observation_period op ON q.person_id = op.person_id
 AND op.observation_period_start_date <= q.start_date AND op.observation_period_end_date >= q.start_date
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
WHERE c.condition_start_date < to_date(cast(2020 AS string) || '-' || cast(4 AS string) || '-' || cast(1 AS string))
-- end condition occurrence criteria
) a ON a.person_id = p.person_id AND a.start_date >= p.op_start_date AND a.start_date <= p.op_end_date AND a.start_date >= date_add(p.start_date, -21) AND a.start_date <= date_add(p.start_date, 34) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING count(cc.event_id) >= 1
-- end correlated criteria
 ) cq ON e.person_id = cq.person_id AND e.event_id = cq.event_id
 GROUP BY e.person_id, e.event_id
 HAVING count(index_id) >= 2
) g
-- end criteria group
) ac ON ac.person_id = pe.person_id AND ac.event_id = pe.event_id
 ) e
 JOIN observation_period op ON e.person_id = op.person_id AND e.start_date >= op.observation_period_start_date AND e.start_date <= op.observation_period_end_date
 WHERE date_add(op.observation_period_start_date, 0) <= e.start_date AND date_add(e.start_date, 0) <= op.observation_period_end_date
) p
WHERE p.ordinal = 1
-- end primary events
) pe
) qe
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

/* Referenced Tables: ['bxi1x9apstrategy_ends', 'included_events'] */
CREATE TABLE bxi1x9apstrategy_ends
AS
SELECT
event_id, person_id,
 CASE WHEN date_add(start_date, 30) > op_end_date THEN op_end_date ELSE date_add(start_date, 30) END AS end_date
FROM
included_events;

/* Referenced Tables: ['cohort_rows', 'included_events', 'bxi1x9apstrategy_ends'] */
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
-- end date strategy
SELECT event_id, person_id, end_date FROM bxi1x9apstrategy_ends
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

/* Referenced Tables: ['bxi1x9apstrategy_ends'] */
TRUNCATE TABLE bxi1x9apstrategy_ends;

/* Referenced Tables: ['bxi1x9apstrategy_ends'] */
DROP TABLE bxi1x9apstrategy_ends;

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

