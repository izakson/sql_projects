
create MATERIALIZED VIEW company_riskrep.cpr_monitor_loss as
with loss as (
    --Собираем базовую таблицу - выгрузку лоссов со всеми необходимыми параметрами
    SELECT dlp.holding_id,
        dlp.holding_name,
        gwp_holding_size_cluster,
         --Определяем регион. Если проставлен регион в программе, берём его; если регион однозначно проставлен в клинике,
         -- берём его; если проставленных регионов у клиники нет или их много, смотрим на адрес пацента или компании. 
         --Регионы делим на миллионики и не миллионики.
		 case coalesce(r.group_title,
	     	case when clinic_region.clinic_point_regions_groups_count!=1 then null else clinic_region.clinic_point_regions_groups end,
	     	case when coalesce(bp.address, bc.real_address) ilike '%москва%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%московская обл%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%мо,%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%мо %' 
	     		then 'Москва и область'
	     		 when coalesce(bp.address, bc.real_address) ilike '%санкт-п%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%санкт п%' 
	     		or coalesce(bp.address, bc.real_address) ilike '% спб%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%ленинградская обл%' 
	     		then 'Санкт-Петербург и область'
	     		when coalesce(bp.address, bc.real_address) ilike '%новосибирск%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%свердловская%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%екатеринбург%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%татарстан%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%нижегородская%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%нижний%новгород%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%челябинск%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%самар%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%башкортостан%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%ростов%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%омск%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%красноярск%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%воронеж%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%перм%' 
	     		or coalesce(bp.address, bc.real_address) ilike '%волгоград%' 
	     		then 'Регионы-миллионники'
	     		 else 'Регионы' end) when 'Москва и область' then 1 
	     		 					when 'Санкт-Петербург и область' then 2
	     		 					when 'Регионы-миллионники' then 3 
	     		 					when 'Регионы' then 4 end
	     		 					as region,
        --Определяем кластер (ценовую категорию) клиники или клиники+прайслиста.
       case when coalesce(pg1.price_category,pg.price_category) is null and dlp.clinic_legal_id=409 then 'standart'
       else coalesce(pg1.price_category,pg.price_category,'econom') end as price_category,
    --Отдельным типом риска выделяем лаборатории среди АПП - услуги с типом "Лабораторные исследования" в дереве (айди 353) и у клиник, где есть тип клиники "Лаборатория"
    case WHEN dlp.service_type_id = 1 AND reg.registry_record_id is not null 
    --AND dlp.clinictype_titles_str ~~* '%лаборатория%'::text 
    THEN 0
                    ELSE dlp.service_type_id end as service_type_id,
     case WHEN dlp.service_type_id = 1 AND reg.registry_record_id is not null 
     --AND dlp.clinictype_titles_str ~~* '%лаборатория%'::text 
     THEN 'Лаборатория'::text
                    ELSE dlp.service_type end as service_type,
    dlp.patient_id,
    dlp.clinic_legal_id,
    dlp.clinic_legal_title,
    date_trunc('month'::text, dlp.registry_record_date::timestamp with time zone)::date AS registry_month,
    --Датой финализации реестра считаем максимальную дату среди даты создания реестра (загрузки реестра в админку),
    --проставленной даты получения реестра и проставленной даты финализации реестра (береём из консампшна, т.к. там
    --эта дата определяется с помощью бб как последнее изменение записи).
    date_trunc('month'::text, GREATEST(br.recieved_date::timestamp without time zone, br.created_at::timestamp without time zone, br.is_finished_at))::date AS registry_finalized_date,
    bp.priority,
    case when floor(DATE_PART('day', now() - bp.birth_date)/365)<18 then 0
    	when floor(DATE_PART('day', now() - bp.birth_date)/365) between 18 and 48 then 1 
    	when floor(DATE_PART('day', now() - bp.birth_date)/365)>48 then 2
    	end as patient_age,
    count(DISTINCT dlp.registry_record_id) AS services,
    sum(dlp.registry_record_total_price) AS loss,
    sum(dlp.registry_record_total_price_without_franchise) AS loss_fr,
    sum(dlp.registry_record_total_price_after_expertise) AS loss_exp,
    sum(dlp.registry_record_total_price_without_franchise_after_expertise) AS loss_fr_exp
FROM dataset_losses_pure dlp
left join company_adminka_consumption company_patient bp
                on dlp.patient_id=bp.id
       	         left  join company_adminka_consumption company_company bc
                on  bc.id=dlp.company_id
   JOIN company_adminka company_registry br
   --Нужна для определения даты финализации реестра.
    ON dlp.registry_id=br.id
    left join (select cle.id as clinic_id, count(distinct group_title) as clinic_point_regions_groups_count,
	     				string_agg(distinct group_title, ', ') as clinic_point_regions_groups,
	     				count(distinct region.title) as clinic_point_regions_count, 
	     				string_agg(distinct region.title, ', ') as clinic_point_regions 
	     from company_adminka_consumption company_cliniclegalentity cle 
	     left join company_adminka_consumption company_clinic c 
	     	on cle.id=c.legal_id
	     left join company_adminka_consumption company_city city 
	     	on city.id=c.city_id
	     left join company_adminka_consumption company_region region 
	     	on city.region_id=region.id
	     group by 1) clinic_region on dlp.clinic_legal_id=clinic_region.clinic_id
   left join company_adminka_consumption company_region r 
   on dlp.program_region=r.title
	     left join (
        --Определяем кластер (ценовую категорию) клиники и прайслиста для определённых юр.лиц клиник (Медси и Неболит), у которых
        --клиники могут относиться к разным ценовым категориям в зависимости от прайслиста. Если по прайслисту не проставлен кластер,
     	--для Медси берем standart
        select distinct clinic_legal_id,pricelist_id, CASE
                    WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['Эконом'::text, 'Эконом+'::text]) THEN 'econom'::text
                    WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['Стандарт'::text, 'Стандарт+'::text]) THEN 'standart'::text
                    WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['VIP'::text, 'VIP+'::text, 'Супер VIP'::text]) THEN 'vip'::text
                    ELSE case when clinic_legal_id=409 then 'standart'::text
                    	else NULL::text END
                END AS  price_category
       from company_felix.calculator_clinic_legal_pricelists_group pg
        left join ds_datasets.cluster_by_clinic_groups clusters
                on pg.calculator_group_uuid::text = clusters.calculator_group_uuid
        where clinic_legal_id in (409,443)) pg1
           on dlp.clinic_legal_id=pg1.clinic_legal_id
           and dlp.pricelist_id=pg1.pricelist_id
     left join (
        --Определяем кластер (ценовую категорию) клиники для остальных юр.лиц клиник. Клиники с незаполненным кластером
        --относим к эконому (по значениям они ближе всего именно к эконому).
        select distinct clinic_legal_id, CASE
                   WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['Эконом'::text, 'Эконом+'::text]) THEN 'econom'::text
                   WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['Стандарт'::text, 'Стандарт+'::text]) THEN 'standart'::text
                    WHEN clusters.clinic_cluster_crm_title = ANY (ARRAY['VIP'::text, 'VIP+'::text, 'Супер VIP'::text]) THEN 'vip'::text
                    ELSE 'econom'::text
                END AS price_category
        from company_felix.calculator_clinic_legal_pricelists_group pg
        left join ds_datasets.cluster_by_clinic_groups clusters
                on pg.calculator_group_uuid::text = clusters.calculator_group_uuid
        where clinic_legal_id not in (409,443)) pg
           on dlp.clinic_legal_id=pg.clinic_legal_id
left join company_adminka_consumption company_holding bh
      on dlp.holding_id = bh.id
  left join company_riskrep.feature_holding_size_new_materialized fhsnm
        on dlp.holding_id = fhsnm.id
  left join (select registry_record_id from datasets.service_tree_node_exploded stn
                    left join company_adminka company_registryrecordmedicalservice rrms
                    	on stn.id=rrms.medical_service_id where 353= any(stn.path)) reg
                    	on reg.registry_record_id=dlp.registry_record_id
  WHERE dlp.registry_record_date >= '2020-01-01'::date
  and dlp.accounting_type::text = 'fronting_risk'::text
      AND (dlp.contract_type = 'fronting_mixed'::text OR dlp.contract_type = 'fronting_risk'::text)
      --Исключаем тестовые и демо холдинги
      and not bh.exclude_from_all
      --Ислкючаем партнерки
      and not bh.partnership
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14)
select *
from loss;

create MATERIALIZED VIEW company_riskrep.cpr_monitor_loss_moving_current as
    --Вторая методология расчета CPR. Берем все записи реестра, даже с финализацией спустя много месяцев.
    select report_month,
                        holding_id,
                        holding_name,
                        gwp_holding_size_cluster,
                        region,
                        price_category,
                        service_type_id,
                        service_type,
                        clinic_legal_title,
                        priority,
                        patient_age,
                        patient_id,
                        clinic_legal_id,
                        loss,
                        loss_fr,
                        loss_exp,
                        loss_fr_exp,
                        --Добавляем колонку с сочетаними пациента и клиники для текущего месяца, чтобы рассчитывать помесячный CPR
                        case when report_month=registry_month then coalesce(patient_id,0) || '-' || clinic_legal_id else null end as patient_clinic_current_month
                from
                    (
                        --Генерируем таблицу для расчета показателей скользящего года, умножая каждую запись на 12 последних месяцев.
                        select *,
                        generate_series(cpr_monitor_loss.registry_month::timestamp without time zone, cpr_monitor_loss.registry_month + '11 mons'::interval, '1 mon'::interval)::date as report_month
                        from company_riskrep.cpr_monitor_loss) as loss_gen
                where 1=1
                    and report_month >= '2020-12-01'::date
                    and report_month < date_trunc('month', now())::date
                  --  and registry_finalized_date <= report_month

create MATERIALIZED VIEW  company_riskrep.cpr_monitor_loss_moving_0 as
    --Первая методология расчета CPR. Берем только записи реестра с финализацией не позднее чем через месяц после получения реестра.
    select report_month,
                        holding_id,
                        holding_name,
                        gwp_holding_size_cluster,
                        region,
                        price_category,
                        service_type_id,
                        service_type,
                        clinic_legal_title,
                        priority,
                        patient_age,
                        patient_id,
                        clinic_legal_id,
                        loss,
                        loss_fr,
                        loss_exp,
                        loss_fr_exp,
                        --Добавляем колонку с сочетаними пациента и клиники для текущего месяца, чтобы рассчитывать помесячный CPR
                        case when report_month=registry_month then coalesce(patient_id,0) || '-' || clinic_legal_id else null end as patient_clinic_current_month
                from  (
                        --Генерируем таблицу для расчета показателей скользящего года, умножая каждую запись на 12 последних месяцев.
                        select *,
                        generate_series(cpr_monitor_loss.registry_month::timestamp without time zone, cpr_monitor_loss.registry_month + '11 mons'::interval, '1 mon'::interval)::date as report_month
                        from company_riskrep.cpr_monitor_loss) as loss_gen
                where 1=1
                    and report_month >= '2020-12-01'::date
                    and report_month < date_trunc('month', now())::date
                    --and registry_finalized_date <= report_month
                    --Ключевое условие методологии:
                    and registry_finalized_date <= registry_month + INTERVAL '1 month';

create MATERIALIZED VIEW company_riskrep.cpr_monitor_loss_moving_plan_2022_01 as
with stat_for_predict AS (
	--Вычисляем показатели за 2021 год для расчета планового CPR. Считаем средний CPR_0 по типу риска, региону, размеру холдинга, клинике, ценовой категории.
         SELECT loss.service_type_id,
            loss.region,
            loss.gwp_holding_size_cluster,
            loss.clinic_legal_id,
            loss.price_category,
            count(DISTINCT ROW(loss.patient_id, loss.clinic_legal_id)) AS unique_patient_clinic,
            sum(loss.loss) AS loss,
            sum(loss.loss_fr) AS loss_fr,
            sum(loss.loss_exp) AS loss_exp,
            sum(loss.loss_fr_exp) AS loss_fr_exp
           FROM company_riskrep.cpr_monitor_loss as loss
           where date_part('year',loss.registry_month)=2021 and registry_finalized_date <= registry_month + INTERVAL '1 month'
          GROUP BY 1,2,3,4,5
        )
,stat_for_fact as (
--Вычисляем фактические показатели за 2021 год для расчета планового CPR. Считаем помесячный накопительный CPR_0 (без учета 2022 года) по типу риска, региону, размеру холдинга, клинике.
select report_month,
			--holding_id,
            --holding_name,
            gwp_holding_size_cluster,
						region,
						price_category,
						service_type_id,
						service_type,
            			clinic_legal_title,
            			clinic_legal_id,
						count(distinct case when date_part('month',report_month)<date_part('month',registry_month) then ROW(patient_id, clinic_legal_id) end) AS unique_patient_clinic,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss end) as loss,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_fr end) as loss_fr,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_exp end) as loss_exp,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_fr_exp end) as loss_fr_exp	
				from (select registry_month,
							generate_series('2022-01-01', '2022-12-01', '1 mon'::interval)::date as report_month,
							holding_id,
            				holding_name,
            				gwp_holding_size_cluster,
							region,
							price_category,
							service_type_id, 
							service_type,
            				clinic_legal_title,
            				clinic_legal_id,
							patient_id,
							loss,
							loss_fr,
							loss_exp,
							loss_fr_exp
							from company_riskrep.cpr_monitor_loss 
							where registry_month < '2022-01-01'
							and registry_finalized_date <= registry_month + INTERVAL '1 month'
							and date_part('year',registry_month)=2021) s
						group by 1,2,3,4,5,6,7,8
						order by 1)
,loss_moving_plan_step1 AS (
		--Первый шаг расчета планового CPR, на котором определяем вес месяцев с фактическими показателями,
		-- высчитываем фактические показатели и подтягиваем средние CPR (предикты).
         SELECT stat_for_fact.report_month,
            --stat_for_fact.holding_id,
            --stat_for_fact.holding_name,
            stat_for_fact.gwp_holding_size_cluster,
            stat_for_fact.region,
            stat_for_fact.price_category,
            stat_for_fact.service_type_id,
            stat_for_fact.service_type,
            stat_for_fact.clinic_legal_title,
            stat_for_fact.clinic_legal_id,
            stat_for_predict.loss AS loss_predict,
            stat_for_predict.loss_fr AS loss_fr_predict,
            stat_for_predict.loss_exp AS loss_exp_predict,
            stat_for_predict.loss_fr_exp AS loss_fr_exp_predict,
            stat_for_predict.unique_patient_clinic AS unique_patient_clinic_predict,
            date_part('month'::text, stat_for_fact.report_month) AS fact_month_num,
            stat_for_fact.unique_patient_clinic as unique_patient_clinic_fact,
            stat_for_fact.loss as loss_fact,
            stat_for_fact.loss_fr AS loss_fr_fact,
            stat_for_fact.loss_exp AS loss_exp_fact,
            stat_for_fact.loss_fr_exp AS loss_fr_exp_fact
           FROM stat_for_fact
             left JOIN stat_for_predict ON stat_for_fact.service_type_id = stat_for_predict.service_type_id
             AND stat_for_fact.region = stat_for_predict.region
             AND stat_for_fact.gwp_holding_size_cluster = stat_for_predict.gwp_holding_size_cluster
             AND stat_for_fact.clinic_legal_id = stat_for_predict.clinic_legal_id
             AND stat_for_fact.price_category = stat_for_predict.price_category
          WHERE 1 = 1 
          order by 1,2
          )
, loss_moving_plan_step2 AS (
         SELECT loss_moving_plan_step1.report_month,
            null::int as holding_id,
            null::text as holding_name,
            loss_moving_plan_step1.gwp_holding_size_cluster,
            loss_moving_plan_step1.region,
            loss_moving_plan_step1.price_category,
            loss_moving_plan_step1.service_type_id,
            loss_moving_plan_step1.service_type,
            loss_moving_plan_step1.clinic_legal_title, 
            null::int as priority,
            null::int as patient_age,
            loss_moving_plan_step1.unique_patient_clinic_fact::double precision + loss_moving_plan_step1.fact_month_num / 12::double precision * loss_moving_plan_step1.unique_patient_clinic_predict::double precision AS unique_patient_clinic_plan,
            loss_moving_plan_step1.clinic_legal_id,
            coalesce(loss_moving_plan_step1.loss_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_plan,
            coalesce(loss_moving_plan_step1.loss_fr_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_fr_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_fr_plan,
            coalesce(loss_moving_plan_step1.loss_exp_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_exp_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_exp_plan,
            coalesce(loss_moving_plan_step1.loss_fr_exp_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_fr_exp_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_fr_exp_plan,
             null::text as patient_clinic_current_month
           FROM loss_moving_plan_step1
           order by 1
        )
select *
from loss_moving_plan_step2;

create materialized view company_riskrep.cpr_monitor_loss_moving_plan_2022_07 as 
with stat_for_predict AS (
	--Вычисляем показатели за 2021 год для расчета планового CPR. Считаем средний CPR_0 по типу риска, региону, размеру холдинга, клинике, ценовой категории.
         SELECT loss.service_type_id,
            loss.region,
            loss.gwp_holding_size_cluster,
            loss.clinic_legal_id,
            loss.price_category,
            count(DISTINCT ROW(loss.patient_id, loss.clinic_legal_id)) AS unique_patient_clinic,
            sum(loss.loss) AS loss,
            sum(loss.loss_fr) AS loss_fr,
            sum(loss.loss_exp) AS loss_exp,
            sum(loss.loss_fr_exp) AS loss_fr_exp
           FROM company_riskrep.cpr_monitor_loss as loss
           where date_trunc('month',loss.registry_month) between '2021-07-01' and '2022-06-01'
           and registry_finalized_date <= registry_month + INTERVAL '1 month'
          GROUP BY 1,2,3,4,5
        )
,stat_for_fact as (
--Вычисляем фактические показатели за 2021 год для расчета планового CPR. Считаем помесячный накопительный CPR_0 (без учета 2022 года) по типу риска, региону, размеру холдинга, клинике.
select report_month,
			--holding_id,
            --holding_name,
            gwp_holding_size_cluster,
						region,
						price_category,
						service_type_id,
						service_type,
            			clinic_legal_title,
            			clinic_legal_id,
						count(distinct case when date_part('month',report_month)<date_part('month',registry_month) then ROW(patient_id, clinic_legal_id) end) AS unique_patient_clinic,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss end) as loss,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_fr end) as loss_fr,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_exp end) as loss_exp,
 						sum(case when date_part('month',report_month)<date_part('month',registry_month) then loss_fr_exp end) as loss_fr_exp	
				from (select registry_month,
							generate_series('2022-07-01', '2023-06-01', '1 mon'::interval)::date as report_month,
							holding_id,
            				holding_name,
            				gwp_holding_size_cluster,
							region,
							price_category,
							service_type_id, 
							service_type,
            				clinic_legal_title,
            				clinic_legal_id,
							patient_id,
							loss,
							loss_fr,
							loss_exp,
							loss_fr_exp
							from company_riskrep.cpr_monitor_loss loss
							where registry_month < '2022-07-01'
							and registry_finalized_date <= registry_month + INTERVAL '1 month'
							and date_trunc('month',loss.registry_month) between '2021-07-01' and '2022-06-01') s
						group by 1,2,3,4,5,6,7,8
						order by 1)
,loss_moving_plan_step1 AS (
		--Первый шаг расчета планового CPR, на котором определяем вес месяцев с фактическими показателями,
		-- высчитываем фактические показатели и подтягиваем средние CPR (предикты).
         SELECT stat_for_fact.report_month,
            --stat_for_fact.holding_id,
            --stat_for_fact.holding_name,
            stat_for_fact.gwp_holding_size_cluster,
            stat_for_fact.region,
            stat_for_fact.price_category,
            stat_for_fact.service_type_id,
            stat_for_fact.service_type,
            stat_for_fact.clinic_legal_title,
            stat_for_fact.clinic_legal_id,
            stat_for_predict.loss AS loss_predict,
            stat_for_predict.loss_fr AS loss_fr_predict,
            stat_for_predict.loss_exp AS loss_exp_predict,
            stat_for_predict.loss_fr_exp AS loss_fr_exp_predict,
            stat_for_predict.unique_patient_clinic AS unique_patient_clinic_predict,
            case when date_part('month'::text, stat_for_fact.report_month)>6 then date_part('month'::text, stat_for_fact.report_month)-6
            	else date_part('month'::text, stat_for_fact.report_month)+6 end AS fact_month_num,
            stat_for_fact.unique_patient_clinic as unique_patient_clinic_fact,
            stat_for_fact.loss as loss_fact,
            stat_for_fact.loss_fr AS loss_fr_fact,
            stat_for_fact.loss_exp AS loss_exp_fact,
            stat_for_fact.loss_fr_exp AS loss_fr_exp_fact
           FROM stat_for_fact
             left JOIN stat_for_predict ON stat_for_fact.service_type_id = stat_for_predict.service_type_id
             AND stat_for_fact.region = stat_for_predict.region
             AND stat_for_fact.gwp_holding_size_cluster = stat_for_predict.gwp_holding_size_cluster
             AND stat_for_fact.clinic_legal_id = stat_for_predict.clinic_legal_id
             AND stat_for_fact.price_category = stat_for_predict.price_category
          WHERE 1 = 1 
          order by 1,2
          )
, loss_moving_plan_step2 AS (
         SELECT loss_moving_plan_step1.report_month,
            null::int as holding_id,
            null::text as holding_name,
            loss_moving_plan_step1.gwp_holding_size_cluster,
            loss_moving_plan_step1.region,
            loss_moving_plan_step1.price_category,
            loss_moving_plan_step1.service_type_id,
            loss_moving_plan_step1.service_type,
            loss_moving_plan_step1.clinic_legal_title, 
            null::int as priority,
            null::int as patient_age,
            loss_moving_plan_step1.unique_patient_clinic_fact::double precision + loss_moving_plan_step1.fact_month_num / 12::double precision * loss_moving_plan_step1.unique_patient_clinic_predict::double precision AS unique_patient_clinic_plan,
            loss_moving_plan_step1.clinic_legal_id,
            coalesce(loss_moving_plan_step1.loss_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_plan,
            coalesce(loss_moving_plan_step1.loss_fr_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_fr_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_fr_plan,
            coalesce(loss_moving_plan_step1.loss_exp_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_exp_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_exp_plan,
            coalesce(loss_moving_plan_step1.loss_fr_exp_fact,0) + loss_moving_plan_step1.fact_month_num / 12 * loss_moving_plan_step1.loss_fr_exp_predict * (1::numeric +
                CASE
                    WHEN loss_moving_plan_step1.service_type_id = 2 THEN 0.2
                    ELSE 0.04
                END)::double precision as loss_fr_exp_plan,
             null::text as patient_clinic_current_month
           FROM loss_moving_plan_step1
           order by 1
        )
select *
from loss_moving_plan_step2;

CREATE MATERIALIZED VIEW company_riskrep.cpr_monitor_dataset_mv as 
select 'cpr_0' as cprtype,
    loss_moving_0.*
from company_riskrep.cpr_monitor_loss_moving_0 as loss_moving_0
union all
select
    'cpr_curr' as cprtype,
    loss_moving_current.*
from company_riskrep.cpr_monitor_loss_moving_current as loss_moving_current
union all
select
    'cpr_plan_2022_01' as cprtype,
    loss_moving_plan_2022_01.*
from company_riskrep.cpr_monitor_loss_moving_plan_2022_01 as loss_moving_plan_2022_01
union all
select
    'cpr_plan_2022_07' as cprtype,
    loss_moving_plan_2022_07.*
from company_riskrep.cpr_monitor_loss_moving_plan_2022_07 as loss_moving_plan_2022_07;





