-- PageID.properties
select
concat(page_id,
',',
replace(page_name, ',', '，'),
',',
IFNULL(url1, 'null'),
',',
IFNULL(url2, 'null'),
',',
IFNULL(url3,'null'),
',',
IFNULL(regexp1, 'null'),
',',
IFNULL(regexp2, 'null'),
',',
IFNULL(regexp3,'null')) as url
from dw.dim_page where page_id>0 order by page_id;

-- MbPageID.properties
select
    concat(page_id,
    ',',
    replace(page_name, ',', '，'),
    ',',
    page_exp1,
    ',',
    page_exp2
from dw.dim_page a
where page_id>0
and a.del_flag = 0
and a.terminal_lvl1_id = 2
order by page_id;

-- MbActionID.propertie
select
    concat(event_id,
    ',',
    replace(event_name, ',', '，'),
    ',',
    event_exp1,
    ',',
    event_exp2)
from dw.dim_event a
where event_id>0
and a.del_flag = 0
and a.terminal_lvl1_id = 2
order by event_id;

-- PcPageValue.properties
select
concat(page_value,
',',
replace(page_type_id, ',', '，'),
',',
IFNULL(url1, 'null'),
',',
IFNULL(url2, 'null'),
',',
IFNULL(url3,'null'),
',',
IFNULL(regexp1, 'null'),
',',
IFNULL(regexp2, 'null'),
',',
IFNULL(regexp3,'null')) as url
from dw.dim_page a where a.page_type_id > 1 and a.terminal_lvl1_id = 1 order by a.page_type_id