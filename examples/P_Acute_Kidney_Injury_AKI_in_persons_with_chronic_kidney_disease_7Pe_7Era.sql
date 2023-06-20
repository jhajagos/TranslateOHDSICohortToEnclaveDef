CREATE TABLE @temp_database_schema.sv7e3rh5Codesets
USING DELTA
 AS
SELECT
CAST(NULL AS int) AS codeset_id,
	CAST(NULL AS bigint) AS concept_id  WHERE 1 = 0;
WITH insertion_temp AS (
(SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (193782,443611)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (193782,443611)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4146536,4032243,2617342)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (4146536,2617342)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (197320,444044)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (197320,444044)
 and c.invalid_reason is NULL
) I
LEFT JOIN
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (196455,37116834)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (196455,37116834)
 and c.invalid_reason is NULL
) E ON I.concept_id = E.concept_id
WHERE E.concept_id is NULL
) C UNION ALL
SELECT 7 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (1314323)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (1314323)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 8 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (43009086,1304643,1301125)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (43009086,1304643,1301125)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 9 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (42539502,4324887)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (42539502,4324887)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (4324887,4180454,2833286,2877118,4163566,4146256,4082531)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (4324887,4180454,2833286,2877118,4163566,4146256,4082531)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 11 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (46271022,45768812)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (46271022,45768812)
 and c.invalid_reason is NULL
) I
LEFT JOIN
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (443611,193782)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (443611,193782)
 and c.invalid_reason is NULL
) E ON I.concept_id = E.concept_id
WHERE E.concept_id is NULL
) C
) UNION ALL (SELECT codeset_id, concept_id FROM @temp_database_schema.sv7e3rh5Codesets ))
INSERT OVERWRITE TABLE @temp_database_schema.sv7e3rh5Codesets  (codeset_id, concept_id) SELECT * FROM insertion_temp;
CREATE TABLE @temp_database_schema.sv7e3rh5qualified_events
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
 JOIN @temp_database_schema.sv7e3rh5Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) C
-- End Condition Occurrence Criteria
UNION ALL
-- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, date_add(C.procedure_date, 1) as END_DATE,
 C.visit_occurrence_id, C.procedure_date as sort_date
from
(
 select po.*
 FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN @temp_database_schema.sv7e3rh5Codesets cs on (po.procedure_concept_id = cs.concept_id and cs.codeset_id = 7)
) C
-- End Procedure Occurrence Criteria
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
-- End Primary Events
) pe
) QE
;
CREATE TABLE @temp_database_schema.sv7e3rh5Inclusion_0
USING DELTA
AS
SELECT
0 as inclusion_rule_id, person_id, event_id
FROM
(
 select pe.person_id, pe.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.sv7e3rh5Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 11)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= date_add(P.START_DATE, 0) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE @temp_database_schema.sv7e3rh5Inclusion_1
USING DELTA
AS
SELECT
1 as inclusion_rule_id, person_id, event_id
FROM
(
 select pe.person_id, pe.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from @temp_database_schema.sv7e3rh5qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.sv7e3rh5Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 2)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= date_add(P.START_DATE, -365) AND A.START_DATE <= date_add(P.START_DATE, -7) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE @temp_database_schema.sv7e3rh5Inclusion_2
USING DELTA
AS
SELECT
2 as inclusion_rule_id, person_id, event_id
FROM
(
 select pe.person_id, pe.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from @temp_database_schema.sv7e3rh5qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, date_add(C.procedure_date, 1) as END_DATE,
 C.visit_occurrence_id, C.procedure_date as sort_date
from
(
 select po.*
 FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN @temp_database_schema.sv7e3rh5Codesets cs on (po.procedure_concept_id = cs.concept_id and cs.codeset_id = 3)
) C
-- End Procedure Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= date_add(P.START_DATE, -365) AND A.START_DATE <= date_add(P.START_DATE, -8) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) <= 3
-- End Correlated Criteria
UNION ALL
-- Begin Criteria Group
select 1 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, date_add(C.procedure_date, 1) as END_DATE,
 C.visit_occurrence_id, C.procedure_date as sort_date
from
(
 select po.*
 FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN @temp_database_schema.sv7e3rh5Codesets cs on (po.procedure_concept_id = cs.concept_id and cs.codeset_id = 3)
) C
-- End Procedure Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= date_add(P.START_DATE, -365) AND A.START_DATE <= date_add(P.START_DATE, -8) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
 COALESCE(C.DRUG_EXPOSURE_END_DATE, date_add(DRUG_EXPOSURE_START_DATE, C.DAYS_SUPPLY), date_add(C.DRUG_EXPOSURE_START_DATE, 1)) as end_date,
 C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from
(
 select de.*
 FROM @cdm_database_schema.DRUG_EXPOSURE de
JOIN @temp_database_schema.sv7e3rh5Codesets cs on (de.drug_concept_id = cs.concept_id and cs.codeset_id = 8)
) C
-- End Drug Exposure Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= date_add(P.START_DATE, -365) AND A.START_DATE <= date_add(P.START_DATE, -8) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) > 0
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE @temp_database_schema.sv7e3rh5Inclusion_3
USING DELTA
AS
SELECT
3 as inclusion_rule_id, person_id, event_id
FROM
(
 select pe.person_id, pe.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events pe
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM @temp_database_schema.sv7e3rh5qualified_events E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, p.person_id, p.event_id
from @temp_database_schema.sv7e3rh5qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.sv7e3rh5Codesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 9)
) C
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE <= date_add(P.START_DATE, 0) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, p.person_id, p.event_id
from @temp_database_schema.sv7e3rh5qualified_events p
LEFT JOIN (
SELECT p.person_id, p.event_id
FROM @temp_database_schema.sv7e3rh5qualified_events P
JOIN (
 -- Begin Procedure Occurrence Criteria
select C.person_id, C.procedure_occurrence_id as event_id, C.procedure_date as start_date, date_add(C.procedure_date, 1) as END_DATE,
 C.visit_occurrence_id, C.procedure_date as sort_date
from
(
 select po.*
 FROM @cdm_database_schema.PROCEDURE_OCCURRENCE po
JOIN @temp_database_schema.sv7e3rh5Codesets cs on (po.procedure_concept_id = cs.concept_id and cs.codeset_id = 10)
) C
-- End Procedure Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE <= date_add(P.START_DATE, 0) ) cc on p.person_id = cc.person_id and p.event_id = cc.event_id
GROUP BY p.person_id, p.event_id
HAVING COUNT(cc.event_id) = 0
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;
CREATE TABLE @temp_database_schema.sv7e3rh5inclusion_events
USING DELTA
AS
SELECT
inclusion_rule_id, person_id, event_id
FROM
(select inclusion_rule_id, person_id, event_id from @temp_database_schema.sv7e3rh5Inclusion_0
UNION ALL
select inclusion_rule_id, person_id, event_id from @temp_database_schema.sv7e3rh5Inclusion_1
UNION ALL
select inclusion_rule_id, person_id, event_id from @temp_database_schema.sv7e3rh5Inclusion_2
UNION ALL
select inclusion_rule_id, person_id, event_id from @temp_database_schema.sv7e3rh5Inclusion_3) I;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5Inclusion_0;
DROP TABLE @temp_database_schema.sv7e3rh5Inclusion_0;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5Inclusion_1;
DROP TABLE @temp_database_schema.sv7e3rh5Inclusion_1;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5Inclusion_2;
DROP TABLE @temp_database_schema.sv7e3rh5Inclusion_2;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5Inclusion_3;
DROP TABLE @temp_database_schema.sv7e3rh5Inclusion_3;
CREATE TABLE @temp_database_schema.sv7e3rh5included_events
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
 from @temp_database_schema.sv7e3rh5qualified_events Q
 LEFT JOIN @temp_database_schema.sv7e3rh5inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
 GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
 ) MG -- matching groups
 -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
 WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),4)-1)
) Results
;
CREATE TABLE @temp_database_schema.sv7e3rh5strategy_ends
USING DELTA
AS
SELECT
event_id, person_id,
 case when date_add(end_date, 7) > op_end_date then op_end_date else date_add(end_date, 7) end as end_date
FROM
@temp_database_schema.sv7e3rh5included_events;
CREATE TABLE @temp_database_schema.sv7e3rh5cohort_rows
USING DELTA
AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
 select F.person_id, F.start_date, F.end_date
 FROM (
 select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
 from @temp_database_schema.sv7e3rh5included_events I
 join ( -- cohort_ends
-- cohort exit dates
-- End Date Strategy
SELECT event_id, person_id, end_date from @temp_database_schema.sv7e3rh5strategy_ends
 ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
 ) F
 WHERE F.ordinal = 1
) FE;
CREATE TABLE @temp_database_schema.sv7e3rh5final_cohort
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
 FROM @temp_database_schema.sv7e3rh5cohort_rows c
 JOIN ( -- cteEndDates
 SELECT
 person_id
 , date_add(event_date, -1 * 7) as end_date
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
 FROM @temp_database_schema.sv7e3rh5cohort_rows
 UNION ALL
 SELECT
 person_id
 , date_add(end_date, 7) as end_date
 , 1 AS event_type
 FROM @temp_database_schema.sv7e3rh5cohort_rows
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
FROM @temp_database_schema.sv7e3rh5final_cohort CO
) UNION ALL (SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM @target_database_schema.@target_cohort_table ))
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT * FROM insertion_temp;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5strategy_ends;
DROP TABLE @temp_database_schema.sv7e3rh5strategy_ends;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5cohort_rows;
DROP TABLE @temp_database_schema.sv7e3rh5cohort_rows;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5final_cohort;
DROP TABLE @temp_database_schema.sv7e3rh5final_cohort;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5inclusion_events;
DROP TABLE @temp_database_schema.sv7e3rh5inclusion_events;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5qualified_events;
DROP TABLE @temp_database_schema.sv7e3rh5qualified_events;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5included_events;
DROP TABLE @temp_database_schema.sv7e3rh5included_events;
TRUNCATE TABLE @temp_database_schema.sv7e3rh5Codesets;
DROP TABLE @temp_database_schema.sv7e3rh5Codesets;