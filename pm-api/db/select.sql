SELECT base.cell_name AS cell_name, base.record_time AS record_time, base.ne_id AS ne_id, base.location AS location, base.cell_index AS cell_index,
       round(SUM(c_68), 2) AS "pm.5G.F1APCounter",
       round(SUM(c_69), 2) AS "pm.5G.F1APCounter_69",round(SUM(c_1050), 2) AS "pm.5G.CellDownTimeManual",
       round(SUM(c_68 + c_1050),2) AS "single_kpi",
       round(SUM(c_69 + c_1049),2) AS "double_kpi",
       (single_kpi * double_kpi) as "third_kpi",
       (third_kpi / double_kpi ) AS "pm.multi_kpi"
FROM (
SELECT record_time, ne_id, AVG(c_1050) AS "c_1050", AVG(c_1049) AS "c_1049",  cell_name, record_time, ne_id, location, cell_index FROM g_nr_cell_availability
WHERE record_time >= '2025-05-23 00:00:00' AND record_time < '2025-06-24 00:00:00'  AND ne_id IN(1000000000, 1000000001)
GROUP BY cell_name, record_time, ne_id, location, cell_index
) AS base FULL JOIN (SELECT record_time, ne_id, AVG(c_68) AS "c_68", AVG(c_69) AS "c_69", cell_name, record_time, ne_id, location, cell_index
 FROM g_nr_flap_interface_signalling
WHERE record_time >= '2025-05-23 00:00:00' AND record_time < '2025-06-24 00:00:00'  AND ne_id IN(1000000000, 1000000001)
GROUP BY cell_name, record_time, ne_id, location, cell_index
) AS gnfis ON base.record_time = gnfis.record_time
AND base.ne_id = gnfis.ne_id
AND base.cell_name = gnfis.cell_name
AND base.location = gnfis.location
AND base.cell_index = gnfis.cell_index
GROUP BY  cell_name, record_time, ne_id, location, cell_index
ORDER BY cell_name, record_time, ne_id, location, cell_index;