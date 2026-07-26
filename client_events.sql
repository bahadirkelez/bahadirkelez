/* @bruin

type: sf.sql
depends: bus_queue.client.raw_client_events
@bruin */

use role sysadmin;
use warehouse BUS_QUEUE_ETL_SERVICE_XSMALL;

-- create table from scratch
create table if not exists bus_queue.client.client_events cluster by (event_name, dt) as																																												
with clean_raw_data as -- exclude auto-collected screen_view event																									
(																									
select																									
run_dt,																									
file_name,																									
raw_data																									
from 
bus_queue.client.raw_client_events																																																	
where 
to_date(to_timestamp_ntz(left(raw_data:event_timestamp::int, 10))) <= '{{ start_date }}'::date        
and raw_data:event_name::string <> 'screen_view'																								
),

clean_raw_data_with_hash as -- extract each unique event from the clean_raw_data																									
(																									
select																									
run_dt,																									
file_name,																									
raw_data,																									
hash(concat(raw_data:user_pseudo_id::string, left(raw_data:event_timestamp::string,10), raw_data:event_name::string)) as hash																									
from 
clean_raw_data																									
qualify row_number() over (partition by hash order by hash asc) = 1																									
)																									
/*																									
raw_client_events_flatten_event_params as																									
(																									
select																									
run_dt,																									
file_name,																									
hash,																									
raw_data,																									
raw_data:user_pseudo_id as user_pseudo_id,																									
null as user_properties,																									
fl.VALUE as event_params,																									
fl.VALUE['key']::VARCHAR as event_params_key,																									
coalesce(fl.VALUE['value']:string_value::STRING, fl.VALUE['value']:int_value::STRING, fl.VALUE['value']:float_value::STRING, fl.VALUE['value']:double_value::STRING, ' ') as event_params_value																									
from																									
clean_raw_data_with_hash h, LATERAL FLATTEN (INPUT => h.raw_data:event_params) fl																									
),																									
																									
raw_client_events as																									
(																									
select																									
run_dt,																									
file_name,																									
hash,																									
raw_data,																									
null as user_properties,																									
object_agg(event_params_key, event_params_value::VARIANT) over (partition by hash) as event_params																									
from																									
raw_client_events_flatten_event_params																									
qualify row_number() over (partition by hash order by hash asc) = 1																									
)																									
*/																									
select																									
hash,																									
raw_data:app_info:id::string as app_id,																									
raw_data:user_pseudo_id::string as firebase_user_id,	
coalesce(raw_data:user_id::string, raw_data:user_properties:user_id::string, raw_data:event_params:user_id::string) as user_id,														
lower(raw_data:platform::string) as platform,																									
raw_data:geo:country::string as country_code,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as event_timestamp,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as client_timestamp,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as client_local_timestamp,																								
raw_data:event_name::string as event_name,																									
raw_data:app_info:version::string as app_version,																									
'' as event_index,																									
raw_data as payload,																									
raw_data:user_properties as user_properties,																									
raw_data:event_params as event_params,																										
current_timestamp::timestamp as load_timestamp,																									
to_date(event_timestamp) as dt,																									
file_name																									
from 
clean_raw_data_with_hash																									
;			



-- delete for event_date = start_date
delete from bus_queue.client.client_events 
where dt between '{{ start_date }}'::date and '{{ start_date }}'::date
;

-- insert for event_date <= start_date using files with run_dt between start_date & start_date + 1
insert into bus_queue.client.client_events
with clean_raw_data as -- exclude auto-collected screen_view event																									
(																									
select																									
run_dt,																									
file_name,																									
raw_data																									
from 
bus_queue.client.raw_client_events																																																	
where 
run_dt between '{{ start_date }}'::date and '{{ start_date }}'::date + 1 -- filter for start_date & start_date + 1 files' data
and to_date(to_timestamp_ntz(left(raw_data:event_timestamp::int, 10))) <= '{{ start_date }}'::date -- filter for event_date <= start_date       
and raw_data:event_name::string <> 'screen_view'																								
),

clean_raw_data_with_hash as -- extract each unique event from the clean_raw_data																									
(																									
select																									
run_dt,																									
file_name,																									
raw_data,																									
hash(concat(raw_data:user_pseudo_id::string, left(raw_data:event_timestamp::string,10), raw_data:event_name::string)) as hash																									
from 
clean_raw_data		
where 
hash not in (select hash from bus_queue.client.client_events where dt >= '{{ start_date }}'::date - 2) -- start_date & start_date+1 files' data may include data with event_date>=start_date-2. To avoid duplicates, we avoid inserting the data with event_date>=start_date-2. bq analytics_events table may include at most 3 days before's data (in terms of event_date)
qualify row_number() over (partition by hash order by hash) = 1																									
)	
/*
raw_client_events_flatten_event_params as																									
(																									
select																									
run_dt,																									
file_name,																									
hash,																									
raw_data,																									
raw_data:user_pseudo_id as user_pseudo_id,																									
null as user_properties,																									
fl.VALUE as event_params,																									
fl.VALUE['key']::VARCHAR as event_params_key,																									
coalesce(fl.VALUE['value']:string_value::STRING, fl.VALUE['value']:int_value::STRING, fl.VALUE['value']:float_value::STRING, fl.VALUE['value']:double_value::STRING, ' ') as event_params_value																									
from																									
clean_raw_data_with_hash h, LATERAL FLATTEN (INPUT => h.raw_data:event_params) fl																									
),		

raw_client_events as																									
(																									
select																									
run_dt,																									
file_name,																									
hash,																									
raw_data,																									
null as user_properties,																									
object_agg(event_params_key, event_params_value::VARIANT) over (partition by hash) as event_params																									
from																									
raw_client_events_flatten_event_params																									
qualify row_number() over (partition by hash order by hash asc) = 1																									
)	
*/
select																									
hash,																									
raw_data:app_info:id::string as app_id,																									
raw_data:user_pseudo_id::string as firebase_user_id,	
coalesce(raw_data:user_id::string, raw_data:user_properties:user_id::string, raw_data:event_params:user_id::string) as user_id,														
lower(raw_data:platform::string) as platform,																									
raw_data:geo:country::string as country_code,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as event_timestamp,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as client_timestamp,																									
to_timestamp_ntz(left(raw_data:event_timestamp::int, 10)) as client_local_timestamp,																								
raw_data:event_name::string as event_name,																									
raw_data:app_info:version::string as app_version,																									
'' as event_index,																									
raw_data as payload,																									
raw_data:user_properties as user_properties,																									
raw_data:event_params as event_params,																										
current_timestamp::timestamp as load_timestamp,																									
to_date(event_timestamp) as dt,																									
file_name																									
from 
clean_raw_data_with_hash																									
;	