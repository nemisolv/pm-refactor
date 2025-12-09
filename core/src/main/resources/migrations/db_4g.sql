create table pm_access_4g.counter
(
    id                     int                                  not null
        primary key,
    name                   varchar(200)                         not null,
    counter_cat_id         int                                  null,
    formula                text                                 null,
    description            text                                 null,
    version                varchar(20)                          null,
    created_by             varchar(50)                          null,
    created_date           datetime default current_timestamp() null,
    units                  varchar(30)                          null,
    status                 smallint default 0                   null,
    kpi_type               int      default 0                   null,
    original_formula       text                                 null,
    arr_counter            text                                 null,
    is_kpi                 tinyint(1)                           null,
    `interval`             int                                  null,
    is_single_group        tinyint(1)                           null,
    arr_group              text                                 null,
    arr_kpi                text                                 null,
    account_id             int                                  null,
    is_peak_hour           tinyint(1)                           null,
    position               int                                  null comment 'Vi tri cua counter trong mang
- null: Counter thuong
- #: Counter ORAN',
    measurement_identifier int                                  null comment '-: Counter thuong
- #: Counter ORAN',
    measurement_object     varchar(100)                         null comment '-: Counter thuong
- #: Counter ORAN',
    measurement_group      varchar(100)                         null comment '-: Counter thuong
- #: Counter ORAN',
    kpi_formula            text                                 null,
    is_sub_kpi             tinyint  default 0                   not null,
    sk_formula             text                                 null,
    sk_cat_id              int                                  null,
    updated_date           timestamp                            null on update current_timestamp(),
    unit_id                int                                  null,
    constraint id_UNIQUE
        unique (id)
)
    collate = utf8mb3_bin;

create table pm_access_4g.counter_cat
(
    id              int auto_increment
        primary key,
    object_level_id int               not null,
    name            varchar(200)      not null,
    code            varchar(200)      null,
    level           tinyint           null,
    parent_id       int               null,
    description     varchar(200)      null,
    version         varchar(50)       null,
    created_by      varchar(50)       null,
    created_date    timestamp         null,
    is_sub_kpi_cat  tinyint default 0 null,
    updated_date    timestamp         null on update current_timestamp(),
    constraint id_UNIQUE
        unique (id)
)
    collate = utf8mb3_bin;

create table pm_access_4g.counter_unit
(
    unit_id   int          not null
        primary key,
    unit_name varchar(255) null,
    version   varchar(255) null,
    object_id int          null,
    enable    bit          null,
    constraint counter_unit_pk
        unique (unit_id),
    constraint counter_unit_pk2
        unique (unit_name)
)
    collate = utf8mb3_unicode_ci;

create table pm_access_4g.extra_field
(
    id              int auto_increment
        primary key,
    object_level_id int               not null,
    column_code     varchar(50)       not null,
    column_name     varchar(50)       null,
    display_name    varchar(128)      null,
    column_type     varchar(50)       not null,
    description     varchar(200)      null,
    is_visible      tinyint default 1 not null,
    is_crud         tinyint default 1 not null,
    created_by      varchar(50)       null,
    created_date    timestamp         null,
    is_key          tinyint default 1 null,
    ne_name         varchar(28)       null,
    constraint extra_field_pk
        unique (object_level_id, column_code),
    constraint extra_field_pk_2
        unique (object_level_id, column_name)
)
    comment 'Phục vụ cho mục đích gửi báo cáo định kỳ' collate = utf8mb3_bin;

create table pm_access_4g.ftp_server
(
    id         int auto_increment
        primary key,
    host       varchar(255)                          not null,
    port       int                                   not null,
    user_name  varchar(255)                          not null,
    password   varchar(255)                          not null,
    created_at timestamp default current_timestamp() null,
    updated_at timestamp default current_timestamp() null on update current_timestamp()
);

create table pm_access_4g.ftp_path
(
    id            int auto_increment
        primary key,
    ftp_server_id int                                   not null,
    path          varchar(255)                          not null,
    created_at    timestamp default current_timestamp() null,
    updated_at    timestamp default current_timestamp() null on update current_timestamp(),
    code          varchar(200)                          null,
    name          varchar(200)                          null,
    constraint ftp_path_ibfk_1
        foreign key (ftp_server_id) references pm_access_4g.ftp_server (id)
);

create index ftp_server_id
    on pm_access_4g.ftp_path (ftp_server_id);

create table pm_access_4g.g_lte_cell_availability
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5050        bigint   default 0                   null,
    c_5051        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_cell_availability_idx
    on pm_access_4g.g_lte_cell_availability (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                             cell_index, relation_cell(120));

create index g_lte_cell_availability_ne_id
    on pm_access_4g.g_lte_cell_availability (ne_id);

create table pm_access_4g.g_lte_cell_utilization
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5060        bigint   default 0                   null,
    c_5061        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_cell_utilization_idx
    on pm_access_4g.g_lte_cell_utilization (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                            cell_index, relation_cell(120));

create index g_lte_cell_utilization_ne_id
    on pm_access_4g.g_lte_cell_utilization (ne_id);

create table pm_access_4g.g_lte_csfb_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5030        bigint   default 0                   null,
    c_5031        bigint   default 0                   null,
    c_5032        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_csfb_measurement_idx
    on pm_access_4g.g_lte_csfb_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                            cell_index, relation_cell(120));

create index g_lte_csfb_measurement_ne_id
    on pm_access_4g.g_lte_csfb_measurement (ne_id);

create table pm_access_4g.g_lte_erab_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5010        bigint   default 0                   null,
    c_5011        bigint   default 0                   null,
    c_5012        bigint   default 0                   null,
    c_5013        bigint   default 0                   null,
    c_5014        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_erab_measurement_idx
    on pm_access_4g.g_lte_erab_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                            cell_index, relation_cell(120));

create index g_lte_erab_measurement_ne_id
    on pm_access_4g.g_lte_erab_measurement (ne_id);

create table pm_access_4g.g_lte_erab_modify
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_erab_modify_idx
    on pm_access_4g.g_lte_erab_modify (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                       relation_cell(120));

create index g_lte_erab_modify_ne_id
    on pm_access_4g.g_lte_erab_modify (ne_id);

create table pm_access_4g.g_lte_inter_rat_handover
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5024        bigint   default 0                   null,
    c_5025        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_inter_rat_handover_idx
    on pm_access_4g.g_lte_inter_rat_handover (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                              cell_index, relation_cell(120));

create index g_lte_inter_rat_handover_ne_id
    on pm_access_4g.g_lte_inter_rat_handover (ne_id);

create table pm_access_4g.g_lte_intra_rat_handover
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5020        bigint   default 0                   null,
    c_5021        bigint   default 0                   null,
    c_5022        bigint   default 0                   null,
    c_5023        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_intra_rat_handover_idx
    on pm_access_4g.g_lte_intra_rat_handover (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                              cell_index, relation_cell(120));

create index g_lte_intra_rat_handover_ne_id
    on pm_access_4g.g_lte_intra_rat_handover (ne_id);

create table pm_access_4g.g_lte_mac_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5040        bigint   default 0                   null,
    c_5041        bigint   default 0                   null,
    c_5042        bigint   default 0                   null,
    c_5043        bigint   default 0                   null,
    c_5044        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_mac_measurement_idx
    on pm_access_4g.g_lte_mac_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                           relation_cell(120));

create index g_lte_mac_measurement_ne_id
    on pm_access_4g.g_lte_mac_measurement (ne_id);

create table pm_access_4g.g_lte_mimo_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_mimo_measurement_idx
    on pm_access_4g.g_lte_mimo_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                            cell_index, relation_cell(120));

create index g_lte_mimo_measurement_ne_id
    on pm_access_4g.g_lte_mimo_measurement (ne_id);

create table pm_access_4g.g_lte_paging_measurement
(
    record_time  datetime                             not null,
    ne_id        int                                  not null,
    duration     int      default 300                 not null,
    created_date datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id)
);

create index g_lte_paging_measurement_idx
    on pm_access_4g.g_lte_paging_measurement (record_time, ne_id);

create index g_lte_paging_measurement_ne_id
    on pm_access_4g.g_lte_paging_measurement (ne_id);

create table pm_access_4g.g_lte_pdcp_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_pdcp_measurement_idx
    on pm_access_4g.g_lte_pdcp_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                            cell_index, relation_cell(120));

create index g_lte_pdcp_measurement_ne_id
    on pm_access_4g.g_lte_pdcp_measurement (ne_id);

create table pm_access_4g.g_lte_rlc_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_rlc_measurement_idx
    on pm_access_4g.g_lte_rlc_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                           relation_cell(120));

create index g_lte_rlc_measurement_ne_id
    on pm_access_4g.g_lte_rlc_measurement (ne_id);

create table pm_access_4g.g_lte_rrc_measurement
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5001        bigint   default 0                   null,
    c_5002        bigint   default 0                   null,
    c_5003        bigint   default 0                   null,
    c_5004        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_rrc_measurement_idx
    on pm_access_4g.g_lte_rrc_measurement (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                           relation_cell(120));

create index g_lte_rrc_measurement_ne_id
    on pm_access_4g.g_lte_rrc_measurement (ne_id);

create table pm_access_4g.g_lte_rrc_setup_time
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_rrc_setup_time_idx
    on pm_access_4g.g_lte_rrc_setup_time (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                          relation_cell(120));

create index g_lte_rrc_setup_time_ne_id
    on pm_access_4g.g_lte_rrc_setup_time (ne_id);

create table pm_access_4g.g_lte_s1_interface_signalling
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_s1_interface_signalling_idx
    on pm_access_4g.g_lte_s1_interface_signalling (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                                   cell_index, relation_cell(120));

create index g_lte_s1_interface_signalling_ne_id
    on pm_access_4g.g_lte_s1_interface_signalling (ne_id);

create table pm_access_4g.g_lte_signal_quality
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    c_5070        bigint   default 0                   null,
    c_5071        bigint   default 0                   null,
    c_5072        bigint   default 0                   null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_signal_quality_idx
    on pm_access_4g.g_lte_signal_quality (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index,
                                          relation_cell(120));

create index g_lte_signal_quality_ne_id
    on pm_access_4g.g_lte_signal_quality (ne_id);

create table pm_access_4g.g_lte_x2_interface_signalling
(
    record_time   datetime                             not null,
    ne_id         int                                  not null,
    duration      int      default 300                 not null,
    cell_name     varchar(200)                         null,
    rat_type      varchar(200)                         null,
    location      varchar(200)                         null,
    cell_index    int                                  null,
    relation_cell varchar(200)                         null,
    created_date  datetime default current_timestamp() not null,
    constraint idx
        unique (record_time, ne_id, cell_name(120), rat_type(120), location(120), cell_index, relation_cell(120))
);

create index g_lte_x2_interface_signalling_idx
    on pm_access_4g.g_lte_x2_interface_signalling (record_time, ne_id, cell_name(120), rat_type(120), location(120),
                                                   cell_index, relation_cell(120));

create index g_lte_x2_interface_signalling_ne_id
    on pm_access_4g.g_lte_x2_interface_signalling (ne_id);

create table pm_access_4g.ne
(
    id           int auto_increment
        primary key,
    created_by   varchar(50)                           null,
    created_date timestamp default current_timestamp() null,
    ip_address   varchar(50)                           null,
    is_active    tinyint                               not null,
    name         varchar(50)                           null,
    ne_type      int                                   not null,
    status       int                                   not null,
    sys_type     int                                   not null,
    vendor_name  varchar(100)                          null,
    ne_group     varchar(50)                           null
)
    collate = utf8mb4_unicode_520_ci;

create table pm_access_4g.ne_group
(
    id                int          not null
        primary key,
    account_id        int          null,
    created_by        varchar(50)  null,
    created_date      timestamp    null,
    description       varchar(100) null,
    group_name        varchar(50)  null,
    is_system_defined tinyint      null,
    last_modified     timestamp    null,
    level             int          null,
    ne_system_type    int          not null,
    number_of_nes     int          null,
    parent_id         int          not null,
    root_id           int          not null,
    condition_param   varchar(200) null,
    code              text         null
)
    collate = utf8mb3_general_ci;

create index site_neGroup_idx
    on pm_access_4g.ne_group (ne_system_type);

create table pm_access_4g.ne_type
(
    id           int          null,
    name         varchar(255) null,
    system_type  int          null,
    description  varchar(255) null,
    created_by   varchar(50)  null,
    created_date timestamp    null
)
    collate = utf8mb4_unicode_520_ci;

create table pm_access_4g.object_level
(
    id           int auto_increment
        primary key,
    name         varchar(200) not null,
    description  varchar(200) null,
    created_by   varchar(50)  null,
    created_date datetime     null,
    constraint id_UNIQUE
        unique (id),
    constraint object_level_pk
        unique (name)
)
    collate = utf8mb3_bin;

create table pm_access_4g.params_code
(
    id           int auto_increment
        primary key,
    type         varchar(50)       null,
    pkey         varchar(50)       null,
    pvalue       varchar(255)      null,
    description  varchar(200)      null,
    is_active    tinyint default 1 not null,
    created_by   varchar(50)       null,
    created_date timestamp         null,
    constraint id_UNIQUE
        unique (id),
    constraint params_code_type_pkey_uindex
        unique (type, pkey)
)
    collate = utf8mb3_bin;

