CREATE OR REPLACE FUNCTION s_grnplm_vd_rozn_mpp_dqc.test_prototip_uat_runcheckactdate(in in_db_name varchar, out out_num_res_val varchar)
	RETURNS varchar
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
     
declare

     v_max_date record;
     v_date_diff_int interval;
     v_date_diff float;
     v_error_msg VARCHAR(512) ;

     ll_on_dt date;
     l_db_name varchar(150) ; 
     l_tab_name varchar(150) ; 
     l_rule_attr_name varchar(150) ; 
     l_rule_sql_expr varchar(2000) ;
     l_rule_interval_type_cd varchar(30) ; 
     l_rule_dt_fld1_name varchar(150) ;
     l_rule_dt_fld2_name varchar(150) ;
     l_rule_where_sql varchar(2000) ;
     l_rule_err_msg varchar(4000) ;
     l_rule_err_code varchar(4000) ;
     l_rule_id integer;

     l_sql_1 varchar(2000) ;
     l_sql_2 varchar(2000) ;
     l_sql_3 varchar(2000) ;
     l_sql_4 varchar(2000) ;
     l_sql_5 varchar(4000) ;

     l_where_condition_2 varchar(4000) ;
     arr_where_condition varchar[] = array[]::varchar[];
     l_where_condition_3 varchar(4000) ;
     l_where_condition_4 varchar(4000) ;
    
     l_combined_where varchar(4000) ;
    
     rec record;
     v_max_date_rec record;
   
     v_combined_sql_1 varchar(4000) ;
     v_combined_sql_2 varchar(4000) ;
     v_combined_sql_3 varchar(4000) ;
     v_combined_sql_4 varchar(4000) ;
     v_combined_sql_5 varchar(4000) ;
    
     v_combined_rule_1 varchar(4000) ;
     v_combined_rule_2 varchar(4000) ;
     v_combined_rule_3 varchar(4000) ;
     v_combined_rule_4 varchar(4000) ;
     v_combined_rule_5 varchar(4000) ;
    
     v_where_condition_2 varchar(4000) ;
     v_where_condition_3 varchar(4000) ;
     v_where_condition_4 varchar(4000) ;
  
     v_combined_sql_final_1 varchar(4000) ;
     v_combined_sql_final_2 varchar(4000) ;
     v_combined_sql_final_3 varchar(4000) ;
     v_combined_sql_final_4 varchar(4000) ;
     v_combined_sql_final_5 varchar(4000) ;
    
    concat_date varchar ;
 	l_rule varchar(4000) ;
    v_max_date_array varchar ;
    query VARCHAR;
 
begin
--select * from s_grnplm_vd_rozn_mpp_dqc.DataMartList_chck_rule;

FOR rec IN (SELECT db_name, tab_name, rule_attr_name, rule_sql_expr, rule_interval_type_cd, rule_dt_fld1_name, rule_dt_fld2_name, rule_where_sql, rule_id, rule_err_msg, rule_err_code, l_on_dt 
--FROM s_grnplm_vd_rozn_mpp_dqc.DataMartList_chck_rule) loop
FROM tmp) loop
        l_rule_attr_name := rec.rule_attr_name;
        l_db_name := rec.db_name;
        l_tab_name := rec.tab_name;
        l_rule_sql_expr := rec.rule_sql_expr;
        l_rule_interval_type_cd := rec.rule_interval_type_cd;
        l_rule_dt_fld1_name := rec.rule_dt_fld1_name;
        l_rule_dt_fld2_name := rec.rule_dt_fld2_name;
        l_rule_where_sql := rec.rule_where_sql;
        l_rule_err_msg := rec.rule_err_msg;
        l_rule_err_code := rec.rule_err_code;
       	l_rule_id := rec.rule_id;
        ll_on_dt := rec.l_on_dt;
      raise notice 'результат  l_rule_id %', l_rule_id;
       
        l_sql_1 := format('coalesce(MAX(%s)::date, date''1900-01-01'') as MaxDate', l_rule_attr_name);
        l_sql_2 := format('coalesce(MAX(%s::date) filter (WHERE $1 BETWEEN %s AND %s AND %s <= %s), date''1900-01-01'') MaxDate', l_rule_attr_name, l_rule_dt_fld1_name, l_rule_dt_fld2_name, Coalesce(l_rule_sql_expr,l_rule_attr_name), Coalesce(l_rule_dt_fld2_name, 'report_dt'));  
     	l_sql_3 := format('coalesce(MAX(%s::date) filter (WHERE %s), date''1900-01-01'') MaxDate', l_rule_attr_name, l_rule_where_sql);  
     	l_sql_4 := format('coalesce(MAX(%s::date) filter (WHERE $1 BETWEEN %s AND %s AND %s <= %s AND %s), date''1900-01-01'') MaxDate', l_rule_attr_name, l_rule_dt_fld1_name, l_rule_dt_fld2_name, Coalesce(l_rule_sql_expr,l_rule_attr_name), Coalesce(l_rule_dt_fld2_name, 'report_dt'), l_rule_where_sql);
        l_sql_5 := format('coalesce((%s), date''1900-01-01'') as MaxDate', l_rule_sql_expr);
       
     	l_rule :=  format('%s', l_rule_id);
     
        l_where_condition_2 := format('($1 BETWEEN %s AND %s AND %s <= %s)', l_rule_dt_fld1_name, l_rule_dt_fld2_name, Coalesce(l_rule_sql_expr,l_rule_attr_name), Coalesce(l_rule_dt_fld2_name, 'report_dt'));  
     	l_where_condition_3 := format('(%s)', l_rule_where_sql);  
     	l_where_condition_4 := format('($1 BETWEEN %s AND %s AND %s <= %s AND %s)', l_rule_dt_fld1_name, l_rule_dt_fld2_name, Coalesce(l_rule_sql_expr,l_rule_attr_name), Coalesce(l_rule_dt_fld2_name, 'report_dt'), l_rule_where_sql); 
     
     raise notice 'результат l_where_condition_2 %', l_where_condition_2;
     raise notice 'результат l_where_condition_3 %', l_where_condition_3;
     raise notice 'результат l_where_condition_4 %', l_where_condition_4;
    
		IF NullIf(rec.rule_sql_expr,'') IS NULL THEN 
        	IF NullIf(rec.rule_where_sql,'') IS NULL THEN
             	IF (rec.rule_dt_fld2_name IS NULL) OR (rec.rule_dt_fld1_name = rec.rule_dt_fld2_name) THEN
       			raise notice 'результат l_sql_1 %', l_sql_1;
        		   v_combined_sql_1 := coalesce(v_combined_sql_1 || ', ','') || l_sql_1;
        		   raise notice 'результат v_combined_sql_1 %', v_combined_sql_1;
        		   v_combined_rule_1 := coalesce(v_combined_rule_1 || ', ', '') || l_rule;
        		   raise notice 'результат v_combined_rule_1 %', v_combined_rule_1;
        		   v_combined_sql_final_1 := ''|| v_combined_sql_1 || ',' || v_combined_rule_1 || '';
				   raise notice 'результат v_combined_sql_final_1 %', v_combined_sql_final_1;
				else
				   v_combined_sql_2 := coalesce(v_combined_sql_2 || ', ', '') || l_sql_2;
				   raise notice 'результат v_combined_sql_2 %', v_combined_sql_2; 
				   v_where_condition_2 := coalesce('(' || l_where_condition_2 || ')', '(1=1)');
				   arr_where_condition := arr_where_condition || array[v_where_condition_2]::varchar[];
				   raise notice 'результат arr_where_condition %', arr_where_condition;
				   raise notice 'результат v_where_condition_2 %', v_where_condition_2;
				   v_combined_rule_2 := coalesce(v_combined_rule_2 || ', ', '') || l_rule;
				    raise notice 'результат v_combined_rule_2 %', v_combined_rule_2;
				   v_combined_sql_final_2 := ''|| v_combined_sql_2 || ',' || v_combined_rule_2 ||'';
				raise notice 'результат v_combined_sql_final_2 %', v_combined_sql_final_2;
		 	    END IF;
		 	else
		 		IF (rec.rule_dt_fld2_name IS NULL) OR (rec.rule_dt_fld1_name = rec.rule_dt_fld2_name) then
		 			v_combined_sql_3 := coalesce(v_combined_sql_3 || ', ', '') || l_sql_3;
		 		    raise notice 'результат v_combined_sql_3 %', v_combined_sql_3;
		 		    v_where_condition_3 := coalesce('(' || l_where_condition_3 || ')', '(1=1)');
				    arr_where_condition := arr_where_condition || array[v_where_condition_3]::varchar[];
		 		    v_where_condition_3 := coalesce(v_where_condition_3 || ' OR ', '') || l_where_condition_3;
				    raise notice 'результат v_where_condition_3 %', v_where_condition_3;
        		    v_combined_rule_3 := coalesce(v_combined_rule_3 || ', ', '') || l_rule;
        		    raise notice 'результат v_combined_rule_3 %', v_combined_rule_3;
		 			v_combined_sql_final_3 := '' || v_combined_sql_3 || ',' || v_combined_rule_3 || '';
		 		    raise notice 'результат v_combined_sql_final_3 %', v_combined_sql_final_3;
		 		else
		 			v_combined_sql_4 := coalesce(v_combined_sql_4 || ', ', '') || l_sql_4;
		 		    raise notice 'результат v_combined_sql_4 %', v_combined_sql_4;
		 		    v_where_condition_4 := coalesce('(' || l_where_condition_4 || ')', '(1=1)');
				    arr_where_condition := arr_where_condition || array[v_where_condition_4]::varchar[];
				    raise notice 'результат v_where_condition_4 %', v_where_condition_4;
		 			v_combined_rule_4 := coalesce(v_combined_rule_4 || ', ', '') || l_rule;
		 			v_combined_sql_final_4 := '' || v_combined_sql_4 || ',' || v_combined_rule_4 || '';
		 		  raise notice 'результат v_combined_sql_final_4 %', v_combined_sql_final_4;
			END IF;
		END IF;
	else
		v_combined_sql_5 := coalesce(v_combined_sql_5 || ', ', '') || l_sql_5;
	    v_combined_rule_5 := coalesce(v_combined_rule_5 || ', ', '') || l_rule;
	    v_combined_sql_final_5 := '' || v_combined_sql_5 || ',' || v_combined_rule_5 || '';
	    raise notice 'результат v_combined_sql_5 %', v_combined_sql_5;
	    raise notice 'результат v_combined_rule_5 %', v_combined_rule_5;
	    raise notice 'результат v_combined_sql_final_5 %', v_combined_sql_final_5;
	END IF;

END LOOP;


query := 'SELECT ';

	if v_combined_sql_final_1 IS NOT null THEN
		query := query || v_combined_sql_final_1 || ', ';
	end IF;

	if v_combined_sql_final_2 IS NOT null THEN
		query := query || v_combined_sql_final_2 || ', ';
	end IF;

	if v_combined_sql_final_3 IS NOT null THEN
		query := query || v_combined_sql_final_3 || ', ';
	end IF;

	if v_combined_sql_final_4 IS NOT null THEN
		query := query || v_combined_sql_final_4 || ', ';
	end IF;

    if v_combined_sql_final_5 IS NOT null THEN
		query := query || v_combined_sql_final_5 || ', ';
	end IF;

-- Удаление последней запятой и добавление "FROM db.tb"
query := LEFT(query, LENGTH(query) -2)|| ' from '||l_db_name||'.'||l_tab_name;
--соединим все фильтры where
--distinct не работает для конкатенаций
select string_agg(distinct where_filter, ' OR ') from unnest(arr_where_condition) as where_filter into l_combined_where;
--l_combined_where := distinct(coalesce(v_where_condition_2, '(1=1)') || ' OR ' || coalesce(v_where_condition_3, '(1=1)') || ' OR ' || coalesce(v_where_condition_4, '(1=1)'));
RAISE notice 'Результат l_combined_where: %', l_combined_where;
query := query || ' WHERE ' || l_combined_where;

RAISE notice 'Результат запроса: %', query;
RAISE notice 'Результат massiv: %', arr_where_condition;
--RAISE notice 'Результат l_combined_where: %', l_combined_where;

			  if query is null then select null into v_max_date;
             ---- raise notice 'результат динамики %', v_max_date; 
              else execute query into v_max_date using ll_on_dt;
            --- raise notice 'результат динамики и v_max_date %, %', query, v_max_date;
              end if;
 
v_max_date_array := array_agg(v_max_date::varchar);
raise notice 'массив v_max_date_array %', v_max_date_array;
       
--развернем параметры типа record и запишем по одному значению в виде двух столбцов - id, date и создадим таблицу (потом мб заменить на tmp)
--drop table if exists s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date;
--create table s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date as 
drop table if exists tmp_concat_id_date;
create temp table tmp_concat_id_date as
WITH dates AS
(
	select ROW_NUMBER() OVER() as Number, v_dates 
	from 
		( 
			select UNNEST(string_to_array(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(t.v_max_date_array,'"',''),'}',''),'{',''),'(',','),')',','),',')) as v_dates 
			FROM (select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(v_max_date_array, '"',''),'}',''),'{',''),'(',','),')',',') as v_max_date_array
		) t
	) tt
	WHERE v_dates != ''
	and v_dates like '%-%'
),
id AS
(
	select ROW_NUMBER() OVER() as Number, id 
	from 
		(
			select UNNEST(string_to_array(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(t.v_max_date_array,'"',''),'}',''),'{',''),'(',','),')',','),',')) as id
			FROM (select REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(v_max_date_array, '"',''),'}',''),'{',''),'(',','),')',',') as v_max_date_array
		) t
	) tt
	WHERE id != ''
	and id not like '%-%'
)
SELECT dates.v_dates, id.id
FROM dates
JOIN id ON dates.number = id.number;  
--select * from s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date

--создадим новую пустую таблицу, чтобы не создавать дубли при insert v_date_diff
--drop table if exists s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_diff; 
--CREATE TABLE s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_diff  (
--drop table if exists tmp_concat_id_date_diff;
--create temp table tmp_concat_id_date_diff (
--    v_max_date text  NULL,
--    id text  NULL,
--    v_date_diff float null,
--    v_rule_interval_type_cd varchar,
--    v_rule_err_msg varchar,
--    v_rule_err_code varchar,
--    v_rule_critical_value_min numeric,
--    v_rule_critical_value_max numeric,
--    v_rule_warning_value_min numeric,
--    v_rule_warning_value_max numeric
--)
--WITH (appendonly=true,compresstype=zstd)
--DISTRIBUTED randomly
--;
--delete from s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_diff;


--вытащим l_rule_interval_type_cd, сделав джойн по rule_id
--drop table if exists s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_type_cd;
--create table s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_type_cd as 
drop table if exists tmp_concat_id_date_type_cd;
create temp table tmp_concat_id_date_type_cd as
SELECT v_dates, id, rule_interval_type_cd, rule_err_msg, rule_err_code, rule_critical_value_min, rule_critical_value_max, rule_warning_value_min, rule_warning_value_max FROM tmp_concat_id_date
--join s_grnplm_vd_rozn_mpp_dqc.DataMartList_chck_rule on DataMartList_chck_rule.rule_id::bigint = dqc_concat_id_date.id::bigint; 
join tmp on tmp.rule_id::bigint = tmp_concat_id_date.id::bigint;


--select * from s_grnplm_vd_rozn_mpp_dqc.dqc_concat_id_date_diff
FOR v_max_date_rec IN SELECT v_dates, id, rule_interval_type_cd, rule_err_msg, rule_err_code, rule_critical_value_min, rule_critical_value_max, rule_warning_value_min, rule_warning_value_max FROM tmp_concat_id_date_type_cd LOOP  
     v_date_diff_int := age(ll_on_dt::date, v_max_date_rec.v_dates::date);
     if v_max_date_rec.rule_interval_type_cd = 'year' THEN 
          v_date_diff := cast(extract(year from v_date_diff_int)*12 + extract(month from v_date_diff_int) as float) / 12;
     elseif v_max_date_rec.rule_interval_type_cd = 'quarter' THEN 
          v_date_diff := cast(extract(year from v_date_diff_int)*12 + extract(month from v_date_diff_int) as float) / 4;
     elseif v_max_date_rec.rule_interval_type_cd = 'month' THEN 
          v_date_diff := extract(year from v_date_diff_int)*12 + extract(month from v_date_diff_int)  + cast(extract(day from v_date_diff_int) as float) / 30;
     elseif v_max_date_rec.rule_interval_type_cd = 'week' THEN 
          v_date_diff := cast(extract(epoch from v_date_diff_int) as float) / (3600*24*7);
     elseif v_max_date_rec.rule_interval_type_cd = 'day' THEN 
          v_date_diff := cast(extract(epoch from v_date_diff_int) as float) / (3600*24);
     end if; 
     raise notice 'значения v_max_date_rec и v_date_diff и rule_interval_type_cd %s, %s, %s', v_max_date_rec, v_date_diff, v_max_date_rec.rule_interval_type_cd;

   
INSERT INTO tmp_concat_id_date_diff (v_max_date, id, v_date_diff, v_rule_interval_type_cd, v_rule_err_msg, v_rule_err_code, v_rule_critical_value_min, v_rule_critical_value_max, v_rule_warning_value_min, v_rule_warning_value_max)  VALUES (v_max_date_rec.v_dates, v_max_date_rec.id, v_date_diff, v_max_date_rec.rule_interval_type_cd, v_max_date_rec.rule_err_msg, v_max_date_rec.rule_err_code, v_max_date_rec.rule_critical_value_min, v_max_date_rec.rule_critical_value_max, v_max_date_rec.rule_warning_value_min, v_max_date_rec.rule_warning_value_max);
END LOOP;
select array_agg(tmp_concat_id_date_diff.v_date_diff) into out_num_res_val from tmp_concat_id_date_diff;
--out_num_res_val := v_date_diff; --тут будет несколько значений, который потом пойдет в таблицу логов
     /*exception 
          when others then 
                v_error_msg := SQLSTATE || ' : ' || SQLERRM ; 
                insert into pcap_sbx_retail_mp.tech_gpm_increment_apply_log values (in_dbname, in_tbname, v_next_incr_dt, v_operation, 0, v_start_ts, current_timestamp(0), v_error_msg) ;
                out_result_msg := v_error_msg ; */ 
    
     
END
;  


$$
EXECUTE ON ANY;
