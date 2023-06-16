CREATE TABLE @temp_database_schema.bxi1x9apCodesets
USING DELTA
 AS
SELECT
CAST(NULL AS int) AS codeset_id,
	CAST(NULL AS bigint) AS concept_id  WHERE 1 = 0;
WITH insertion_temp AS (
(SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (260125,254058,260139,4112521,4110483,4110485,4110023,46273719,46271075,4307774,4051332,36716978,37016114,4112359,4195694,319049,4110484,4112015,257011,312940,254677,442555,4059022,4059021,40482069,256451,256722,4059003,4168213,434490,4140438,314971,439676,254761,4048098,37311061,4100065,320136,4038519,312437,4060052,4263848,37311059,37016200,4011766,437663,4141062,4164645,4047610,46271074,4260205,4310964,37395564,4133224,4185711,4289517,4140453,4090569,439857,4109381,4330445,255848,40482061,436145,40479642,4102774,4256228,320651,436235,261326,3661405,3661406,3662381,756031,37311061,3663281,3661408,756039,320651)
) I
) C UNION ALL
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (9201)
) I
) C UNION ALL
SELECT 5 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (260139,46271075,4307774,4195694,257011,442555,4059022,4059021,256451,4059003,4168213,434490,439676,254761,4048098,37311061,4100065,320136,4038519,312437,4060052,4263848,37311059,37016200,4011766,437663,4141062,4164645,4047610,4260205,4185711,4289517,4140453,4090569,4109381,4330445,255848,4102774,320651,436235,261326,3661406,3662381,756031,3661408,756039,3661405,37311061,37310284,37310283,37310286,3663281,37310287,37310254,4193169,260125)
UNION select c.concept_id
 from @vocabulary_database_schema.CONCEPT c
 join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
 and ca.ancestor_concept_id in (3661406,3662381,756031,3661408,756039,3661405,37311061,37310284,37310283,37310286,3663281,37310287,37310254)
 and c.invalid_reason is NULL
) I
) C UNION ALL
SELECT 6 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
 select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (36661370,706167,706157,706155,36661371,715272,757678,706161,586524,586525,36661378,36032258,586520,706175,706156,706154,723469,706168,723478,36031506,723464,723471,723470,36031652,706160,36032174,706173,36031453,586528,586529,715262,723476,586526,757677,36031238,706163,36661377,715260,715261,723463,706170,706158,36032061,706169,723467,723468,723465,36031213,586519,723466,36031944,586517)
) I
) C
) UNION ALL (SELECT codeset_id, concept_id FROM @temp_database_schema.bxi1x9apCodesets ))
INSERT OVERWRITE TABLE @temp_database_schema.bxi1x9apCodesets  (codeset_id, concept_id) SELECT * FROM insertion_temp;
CREATE TABLE @temp_database_schema.bxi1x9apqualified_events
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
 select PE.person_id, PE.event_id, PE.start_date, PE.end_date, PE.visit_occurrence_id, PE.sort_date FROM (
-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, date_add(C.measurement_date, 1) as END_DATE,
 C.visit_occurrence_id, C.measurement_date as sort_date
from
(
 select m.*
 FROM @cdm_database_schema.MEASUREMENT m
JOIN @temp_database_schema.bxi1x9apCodesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
WHERE C.measurement_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
AND C.value_as_concept_id in (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- End Measurement Criteria
) PE
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
 select E.person_id, E.event_id
 FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, date_add(C.measurement_date, 1) as END_DATE,
 C.visit_occurrence_id, C.measurement_date as sort_date
from
(
 select m.*
 FROM @cdm_database_schema.MEASUREMENT m
JOIN @temp_database_schema.bxi1x9apCodesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
WHERE C.measurement_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
AND C.value_as_concept_id in (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- End Measurement Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id
 and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) E
 INNER JOIN
 (
 -- Begin Correlated Criteria
select 0 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, date_add(C.measurement_date, 1) as END_DATE,
 C.visit_occurrence_id, C.measurement_date as sort_date
from
(
 select m.*
 FROM @cdm_database_schema.MEASUREMENT m
JOIN @temp_database_schema.bxi1x9apCodesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
WHERE C.measurement_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
AND C.value_as_concept_id in (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- End Measurement Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id
 and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.bxi1x9apCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 5)
) C
WHERE C.condition_start_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, -21) AND A.START_DATE <= date_add(P.START_DATE, 34) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 1 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, date_add(C.measurement_date, 1) as END_DATE,
 C.visit_occurrence_id, C.measurement_date as sort_date
from
(
 select m.*
 FROM @cdm_database_schema.MEASUREMENT m
JOIN @temp_database_schema.bxi1x9apCodesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
WHERE C.measurement_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
AND C.value_as_concept_id in (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- End Measurement Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id
 and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
 -- Begin Visit Occurrence Criteria
select C.person_id, C.visit_occurrence_id as event_id, C.visit_start_date as start_date, C.visit_end_date as end_date,
 C.visit_occurrence_id, C.visit_start_date as sort_date
from
(
 select vo.*
 FROM @cdm_database_schema.VISIT_OCCURRENCE vo
JOIN @temp_database_schema.bxi1x9apCodesets cs on (vo.visit_concept_id = cs.concept_id and cs.codeset_id = 4)
) C
-- End Visit Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, -7) AND A.START_DATE <= date_add(P.START_DATE, 21) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
UNION ALL
-- Begin Correlated Criteria
select 2 as index_id, cc.person_id, cc.event_id
from (SELECT p.person_id, p.event_id
FROM (SELECT Q.person_id, Q.event_id, Q.start_date, Q.end_date, Q.visit_occurrence_id, OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date
FROM (-- Begin Measurement Criteria
select C.person_id, C.measurement_id as event_id, C.measurement_date as start_date, date_add(C.measurement_date, 1) as END_DATE,
 C.visit_occurrence_id, C.measurement_date as sort_date
from
(
 select m.*
 FROM @cdm_database_schema.MEASUREMENT m
JOIN @temp_database_schema.bxi1x9apCodesets cs on (m.measurement_concept_id = cs.concept_id and cs.codeset_id = 6)
) C
WHERE C.measurement_date >= to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
AND C.value_as_concept_id in (4126681,45877985,45884084,9191,4181412,45879438,45881802)
-- End Measurement Criteria
) Q
JOIN @cdm_database_schema.OBSERVATION_PERIOD OP on Q.person_id = OP.person_id
 and OP.observation_period_start_date <= Q.start_date and OP.observation_period_end_date >= Q.start_date
) P
JOIN (
 -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, date_add(C.condition_start_date, 1)) as end_date,
 C.visit_occurrence_id, C.condition_start_date as sort_date
FROM
(
 SELECT co.*
 FROM @cdm_database_schema.CONDITION_OCCURRENCE co
 JOIN @temp_database_schema.bxi1x9apCodesets cs on (co.condition_concept_id = cs.concept_id and cs.codeset_id = 0)
) C
WHERE C.condition_start_date < to_date(cast(2020 as string) || '-' || cast(4 as string) || '-' || cast(1 as string))
-- End Condition Occurrence Criteria
) A on A.person_id = P.person_id AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= date_add(P.START_DATE, -21) AND A.START_DATE <= date_add(P.START_DATE, 34) ) cc
GROUP BY cc.person_id, cc.event_id
HAVING COUNT(cc.event_id) >= 1
-- End Correlated Criteria
 ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
 GROUP BY E.person_id, E.event_id
 HAVING COUNT(index_id) >= 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id and AC.event_id = pe.event_id
 ) E
 JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >= OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
 WHERE date_add(OP.OBSERVATION_PERIOD_START_DATE, 0) <= E.START_DATE AND date_add(E.START_DATE, 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events
) pe
) QE
;
CREATE TABLE @temp_database_schema.bxi1x9apinclusion_events
USING DELTA
 AS
SELECT
CAST(NULL AS bigint) AS inclusion_rule_id,
	CAST(NULL AS bigint) AS person_id,
	CAST(NULL AS bigint) AS event_id  WHERE 1 = 0;
CREATE TABLE @temp_database_schema.bxi1x9apincluded_events
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
 from @temp_database_schema.bxi1x9apqualified_events Q
 LEFT JOIN @temp_database_schema.bxi1x9apinclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
 GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
 ) MG -- matching groups
) Results
WHERE Results.ordinal = 1
;
CREATE TABLE @temp_database_schema.bxi1x9apstrategy_ends
USING DELTA
AS
SELECT
event_id, person_id,
 case when date_add(start_date, 30) > op_end_date then op_end_date else date_add(start_date, 30) end as end_date
FROM
@temp_database_schema.bxi1x9apincluded_events;
CREATE TABLE @temp_database_schema.bxi1x9apcohort_rows
USING DELTA
AS
SELECT
person_id, start_date, end_date
FROM
( -- first_ends
 select F.person_id, F.start_date, F.end_date
 FROM (
 select I.event_id, I.person_id, I.start_date, CE.end_date, row_number() over (partition by I.person_id, I.event_id order by CE.end_date) as ordinal
 from @temp_database_schema.bxi1x9apincluded_events I
 join ( -- cohort_ends
-- cohort exit dates
-- End Date Strategy
SELECT event_id, person_id, end_date from @temp_database_schema.bxi1x9apstrategy_ends
 ) CE on I.event_id = CE.event_id and I.person_id = CE.person_id and CE.end_date >= I.start_date
 ) F
 WHERE F.ordinal = 1
) FE;
CREATE TABLE @temp_database_schema.bxi1x9apfinal_cohort
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
 FROM @temp_database_schema.bxi1x9apcohort_rows c
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
 FROM @temp_database_schema.bxi1x9apcohort_rows
 UNION ALL
 SELECT
 person_id
 , date_add(end_date, 0) as end_date
 , 1 AS event_type
 FROM @temp_database_schema.bxi1x9apcohort_rows
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
FROM @temp_database_schema.bxi1x9apfinal_cohort CO
) UNION ALL (SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM @target_database_schema.@target_cohort_table ))
INSERT OVERWRITE TABLE @target_database_schema.@target_cohort_table  (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT * FROM insertion_temp;
TRUNCATE TABLE @temp_database_schema.bxi1x9apstrategy_ends;
DROP TABLE @temp_database_schema.bxi1x9apstrategy_ends;
TRUNCATE TABLE @temp_database_schema.bxi1x9apcohort_rows;
DROP TABLE @temp_database_schema.bxi1x9apcohort_rows;
TRUNCATE TABLE @temp_database_schema.bxi1x9apfinal_cohort;
DROP TABLE @temp_database_schema.bxi1x9apfinal_cohort;
TRUNCATE TABLE @temp_database_schema.bxi1x9apinclusion_events;
DROP TABLE @temp_database_schema.bxi1x9apinclusion_events;
TRUNCATE TABLE @temp_database_schema.bxi1x9apqualified_events;
DROP TABLE @temp_database_schema.bxi1x9apqualified_events;
TRUNCATE TABLE @temp_database_schema.bxi1x9apincluded_events;
DROP TABLE @temp_database_schema.bxi1x9apincluded_events;
TRUNCATE TABLE @temp_database_schema.bxi1x9apCodesets;
DROP TABLE @temp_database_schema.bxi1x9apCodesets;