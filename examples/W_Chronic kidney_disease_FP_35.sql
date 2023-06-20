CREATE TABLE @temp_database_schema.nyx0drrlCodesets
USING DELTA
 AS
SELECT
CAST(NULL AS int) AS codeset_id,
	CAST(NULL AS bigint) AS concept_id  WHERE 1 = 0;
WITH insertion_temp AS (
(SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (194385,46271022,192279,4263367,261071,201313,4103224,193253,195314,192359,45768812)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (194385,46271022,192279,4263367,261071,201313,4103224,195314,192359,45768812)
 and c.invalid_reason is NULL
) I
LEFT JOIN
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (45769152,195289,195737,43530912,4066005,37116834,195014,197930,197320)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (45769152,195289,195737,43530912,4066005,37116834,195014,197930,197320)
 and c.invalid_reason is NULL
) E ON I.concept_id = E.concept_id
WHERE E.concept_id is NULL
) C UNION ALL
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4090651,4032243,45889365,4027133,38003431)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (4090651,4032243,45889365,4027133,38003431)
 and c.invalid_reason is NULL
) I
) C
) UNION ALL (SELECT codeset_id, concept_id FROM @temp_database_schema.nyx0drrlCodesets ))
INSERT OVERWRITE TABLE @temp_database_schema.nyx0drrlCodesets  (codeset_id, concept_id) SELECT * FROM insertion_temp;
CREATE TABLE @temp_database_schema.nyx0drrlqualified_events
USING DELTA
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
FROM
(
 select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, 30) AND A.START_DATE <= P.OP_END_DATE ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) P
JOIN (
 -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, date_add(C.procedure_date, 1) as END_DATE,
 C.visit_occurrence_id, C.procedure_date as sort_date
from
(
 select po.*
 FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN @temp_database_schema.nyx0drrlCodesets cs on (po.procedure_concept_id = cs.concept_id and cs.codeset_id = 1)
) C
-- End Procedure Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, 0) AND A.START_DATE <= P.OP_END_DATE ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 2
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 2 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) P
JOIN (
 -- Begin Observation Criteria
select C.person_id, C.observation_id as event_id, C.observation_date as start_date, date_add(C.observation_date, 1) as END_DATE,
 C.visit_occurrence_id, C.observation_date as sort_date
from
(
 select o.*
 FROM @cdm_database_schema.OBSERVATION o
JOIN @temp_database_schema.nyx0drrlCodesets cs on (o.observation_concept_id = cs.concept_id and cs.codeset_id = 1)
) C
-- End Observation Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, 0) AND A.START_DATE <= P.OP_END_DATE ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 2
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 3 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
 select E.person_id, E.start_date, E.end_date,
 row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC, E.event_id) ordinal,
 OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
 FROM
 (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
-- End Condition Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.nyx0drrlCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 1)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, 0) AND A.START_DATE <= P.OP_END_DATE ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 2
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id
) QE
WHERE QE.ordinal = 1
;
CREATE TABLE @temp_database_schema.nyx0drrlinclusion_events
USING DELTA
 AS
SELECT
CAST(NULL AS bigint) AS inclusion_rule_id,
	CAST(NULL AS bigint) AS person_id,
	CAST(NULL AS bigint) AS event_id  WHERE 1 = 0;
CREATE TABLE @temp_database_schema.nyx0drrlincluded_events
USING DELTA
AS
SELECT
event_id, person_id, start_date, end_date, op_start_date, op_end_date
FROM
(
 SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
 from
 (
 select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
 from @temp_database_schema.nyx0drrlqualified_events Q
 LEFT JOIN @temp_database_schema.nyx0drrlinclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
 GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
 ) MG -- matching groups
) Results
WHERE Results.ordinal = 1
;
CREATE TABLE @temp_database_schema.nyx0drrlcohort_rows
USING DELTA
AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
 select F.person_id, F.start_date, F.end_date
 FROM (
 select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
 from @temp_database_schema.nyx0drrlincluded_events I
 join ( -- cohort_ends
-- cohort exit dates
-- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from @temp_database_schema.nyx0drrlincluded_events
 ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
 ) F
 WHERE F.ordinal = 1
) FE;
CREATE TABLE @temp_database_schema.nyx0drrlfinal_cohort
USING DELTA
AS
SELECT
person_id, min(start_date) as start_date, end_date
FROM
( --cteEnds
 SELECT
 c.person_id
 , c.start_date
 , MIN(ed.end_date) AS end_date
 FROM @temp_database_schema.nyx0drrlcohort_rows c
 JOIN ( -- cteEndDates
 SELECT
 person_id
 , date_add(event_date, -1 * 0) as end_date
 FROM
 (
 SELECT
 person_id
 , event_date
 , event_type
 , SUM(event_type) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS interval_status
 FROM
 (
 SELECT
 person_id
 , start_date AS event_date
 , -1 AS event_type
 FROM @temp_database_schema.nyx0drrlcohort_rows
 UNION ALL
 SELECT
 person_id
 , date_add(end_date, 0) as end_date
 , 1 AS event_type
 FROM @temp_database_schema.nyx0drrlcohort_rows
 ) RAWDATA
 ) e
 WHERE interval_status = 0
 ) ed ON c.person_id = ed.person_id AND ed.end_date >= c.start_date
 GROUP BY c.person_id, c.start_date
) e
group by person_id, end_date
;
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  SELECT * FROM @target_database_schema.@target_cohort_table  WHERE NOT (cohort_definition_id = @target_cohort_id);
WITH insertion_temp AS (
(SELECT @target_cohort_id as cohort_definition_id, person_id, start_date, end_date
FROM @temp_database_schema.nyx0drrlfinal_cohort CO
) UNION ALL (SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM @target_database_schema.@target_cohort_table ))
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT * FROM insertion_temp;
TRUNCATE TABLE @temp_database_schema.nyx0drrlcohort_rows;
DROP TABLE @temp_database_schema.nyx0drrlcohort_rows;
TRUNCATE TABLE @temp_database_schema.nyx0drrlfinal_cohort;
DROP TABLE @temp_database_schema.nyx0drrlfinal_cohort;
TRUNCATE TABLE @temp_database_schema.nyx0drrlinclusion_events;
DROP TABLE @temp_database_schema.nyx0drrlinclusion_events;
TRUNCATE TABLE @temp_database_schema.nyx0drrlqualified_events;
DROP TABLE @temp_database_schema.nyx0drrlqualified_events;
TRUNCATE TABLE @temp_database_schema.nyx0drrlincluded_events;
DROP TABLE @temp_database_schema.nyx0drrlincluded_events;
TRUNCATE TABLE @temp_database_schema.nyx0drrlCodesets;
DROP TABLE @temp_database_schema.nyx0drrlCodesets;