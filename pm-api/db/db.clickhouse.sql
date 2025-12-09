-- pm_access.g_nr_cell_availability definition

CREATE TABLE pm_access.g_nr_cell_availability (
  record_time DateTime,
  duration Int32 DEFAULT 300,
  ne_id Int32,
  cell_name String,
  freq String,
  location String,
  cell_index Int32,
  relation_cell String,
  created_date DateTime DEFAULT now(),
  c_1049 Int64 DEFAULT 0,
  c_1050 Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(record_time)
PRIMARY KEY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
ORDER BY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
SETTINGS index_granularity = 8192;


INSERT INTO pm_access.g_nr_cell_availability (record_time, duration, ne_id, cell_name, freq, location, cell_index, relation_cell, created_date, c_1049, c_1050) VALUES
('2025-07-08 14:55:00', 300, 1007630774, 'gNB0284_10n411', '', 'CellName=gNB0284_10n411', 1, '', '2025-05-27 23:24:54', 20, 76),
('2025-07-08 14:55:00', 300, 1007630774, 'gNB0284_20n411', '', 'CellName=gNB0284_20n411', 2, '', '2025-05-27 23:24:54', 58, 86),
('2025-07-08 14:55:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 08:45:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 58, 86),
('2025-07-08 13:05:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 14:50:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 15:00:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 16:00:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:00:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:25:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:30:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:35:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:45:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5),
('2025-07-08 17:50:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 23:24:54', 92, 5);


-- pm_access.g_nr_flap_interface_signalling definition

CREATE TABLE pm_access.g_nr_flap_interface_signalling (
  record_time DateTime,
  duration Int32 DEFAULT 300,
  ne_id Int32,
  cell_name String,
  freq String,
  location String,
  cell_index Int32,
  relation_cell String,
  created_date DateTime DEFAULT now(),
  c_68 Int64 DEFAULT 0,
  c_69 Int64 DEFAULT 0,
  c_70 Int64 DEFAULT 0,
  c_71 Int64 DEFAULT 0,
  c_72 Int64 DEFAULT 0,
  c_73 Int64 DEFAULT 0,
  c_74 Int64 DEFAULT 0,
  c_75 Int64 DEFAULT 0,
  c_76 Int64 DEFAULT 0,
  c_77 Int64 DEFAULT 0,
  c_78 Int64 DEFAULT 0,
  c_79 Int64 DEFAULT 0,
  c_80 Int64 DEFAULT 0,
  c_81 Int64 DEFAULT 0,
  c_82 Int64 DEFAULT 0,
  c_83 Int64 DEFAULT 0,
  c_84 Int64 DEFAULT 0,
  c_85 Int64 DEFAULT 0,
  c_86 Int64 DEFAULT 0,
  c_87 Int64 DEFAULT 0,
  c_35001 Int64 DEFAULT 0,
  c_35002 Int64 DEFAULT 0,
  c_35003 Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(record_time)
PRIMARY KEY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
ORDER BY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
SETTINGS index_granularity = 8192;

INSERT INTO pm_access.g_nr_flap_interface_signalling (record_time, duration, ne_id, cell_name, freq, location, cell_index, relation_cell, created_date, c_68, c_69) VALUES
('1970-01-01 08:00:00', 300, 84997362, 'gNB052_10n411', '', 'CellName=gNB052_10n411', 1, '', '2025-05-27 17:14:54', 58, 77),
('1970-01-01 08:00:00', 300, 84997362, 'gNB052_20n411', '', 'CellName=gNB052_20n411', 2, '', '2025-05-27 17:14:54', 70, 97),
('1970-01-01 08:00:00', 300, 1007630774, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 15:32:56', 64, 91),
('1970-01-01 08:00:00', 300, 84997362, 'gNB0284_10n411', '', 'CellName=gNB0284_10n411', 1, '', '2025-05-27 17:14:54', 58, 77 ),
('1970-01-01 08:00:00', 300, 84997362, 'gNB0284_20n411', '', 'CellName=gNB0284_20n411', 2, '', '2025-05-27 17:14:54', 87, 39),
('1970-01-01 08:00:00', 300, 84997362, 'gNB0284_30n411', '', 'CellName=gNB0284_30n411', 3, '', '2025-05-27 17:14:54', 58, 77);

-- pm_access.g_nr_rrc_measurement definition
CREATE TABLE pm_access.g_nr_rrc_measurement (
  record_time DateTime,
  duration Int32 DEFAULT 300,
  ne_id Int32,
  cell_name String,
  freq String,
  location String,
  cell_index Int32,
  relation_cell String,
  created_date DateTime DEFAULT now(),
  c_88 Int64 DEFAULT 0,
  c_89 Int64 DEFAULT 0,
  c_90 Int64 DEFAULT 0,
  c_91 Int64 DEFAULT 0,
  c_92 Int64 DEFAULT 0,
  c_93 Int64 DEFAULT 0,
  c_94 Int64 DEFAULT 0,
  c_95 Int64 DEFAULT 0,
  c_96 Int64 DEFAULT 0,
  c_97 Int64 DEFAULT 0,
  c_98 Int64 DEFAULT 0,
  c_99 Int64 DEFAULT 0
) ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(record_time)
PRIMARY KEY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
ORDER BY (ne_id, record_time, cell_name, freq, location, cell_index, relation_cell)
SETTINGS index_granularity = 8192;

INSERT INTO pm_access.g_nr_rrc_measurement (record_time, duration, ne_id, cell_name, freq, location, cell_index, relation_cell, created_date, c_88, c_89, c_90) VALUES
('2025-05-27 10:55:00', 300, 84997362, 'gNB052_10n411', '', 'CellName=gNB052_10n411', 1, '', '2025-05-27 17:14:55', 76, 21, 77),
('2025-05-27 10:55:00', 300, 84997362, 'gNB052_20n411', '', 'CellName=gNB052_20n411', 2, '', '2025-05-27 17:14:55', 57, 46, 31);