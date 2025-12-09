/*
QUERY
SELECT 
    formatDateTime(toDateTime(toStartOfInterval(base.record_time, INTERVAL 1 HOUR)), '%Y-%m-%d %H:%M:%S') as `record_time`,
    base.ne_id, 
    base.cell_index, 
    base.cell_name, 
    base.location, 
    round(SUM(c_1049), 2) AS "pm.5G.CellDownTimeAuto",
    round(SUM(c_1050), 2) AS "pm.5G.CellDownTimeManual",
    round(SUM(c_68), 2) AS "pm.5G.F1APCounter",
    round(SUM(c_35002), 2) AS "pm.5G.Measurement"
FROM (
    SELECT 
        record_time,
        ne_id, 
        cell_name, 
        location, 
        cell_index, 
        AVG(c_1049) AS c_1049, 
        AVG(c_1050) AS c_1050
    FROM g_nr_cell_availability 
    WHERE record_time >= '2022-07-08 15:45:00' AND record_time < '2030-06-24 00:00:00'
--  AND ne_id IN (1007630780, 1007630774, 10999, 10990)
    GROUP BY record_time, ne_id, cell_name, location, cell_index
) base
FULL JOIN (
    SELECT 
        record_time,
        ne_id, 
        cell_name, 
        location, 
        cell_index, 
        AVG(c_68) AS c_68
    FROM g_nr_flap_interface_signalling 
 WHERE record_time >= '2022-07-08 15:45:00' AND record_time < '2030-06-24 00:00:00'
--  AND ne_id IN (1007630780, 1007630774, 10999, 10990)
    GROUP BY cell_name, record_time, ne_id, location, cell_index
) flap
ON base.record_time = flap.record_time
AND base.ne_id = flap.ne_id
AND base.cell_name = flap.cell_name
AND base.location = flap.location
AND base.cell_index = flap.cell_index
FULL JOIN (
    SELECT 
        record_time,
        ne_id, 
        cell_name, 
        location, 
        cell_index, 
        AVG(c_35002 ) AS c_35002
    FROM g_nr_measurement 
 WHERE record_time >= '2022-07-08 15:45:00' AND record_time < '2026-06-24 00:00:00'
--  AND ne_id IN (1007630780, 1007630774, 10999, 10990)
    GROUP BY record_time, ne_id, cell_name, location, cell_index
) rrcm
ON rrcm.record_time = base.record_time
AND rrcm.ne_id = base.ne_id
AND rrcm.cell_name = base.cell_name
AND rrcm.location = base.location
AND rrcm.cell_index = base.cell_index
GROUP BY base.record_time, base.ne_id, base.cell_index, base.cell_name, base.location
ORDER BY base.record_time DESC, base.ne_id, base.cell_index, base.cell_name, base.location;



*/

