



--replies table
drop table p_alexey2_t.day_replies;
create multiset table p_alexey2_t.day_replies as (
select e.clsfd_site_id
	,e.clsfd_ad_id 
	,a.src_cre_dt
	,a.src_cre_tm
	,(cast(a.src_cre_dt  as date) - date '1970-01-01') * 86400
	+ (extract(hour from a.src_cre_tm) * 3600)
	+ (extract(minute from a.src_cre_tm) * 60)+ (extract(second from a.src_cre_tm)) as seconds_listings
	,count(clsfd_event_action) as replies
from clsfd_access_views.clsfd_pi_event as e
inner join clsfd_access_views.dw_clsfd_ad a
on a.clsfd_ad_id = e.clsfd_ad_id and a.clsfd_site_id = e.clsfd_site_id
inner join p_clsfd_t.clsfd_pidiffadtime dif
on e.clsfd_site_id=dif.clsfd_site_id
where e.clsfd_site_id = 201
and e.clsfd_sum_dt >= '2016-01-01' 
and clsfd_event_action in ( 'r2ssmsbegin', 'r2sphonebegin', 'r2semailsuccess', 'r2schatsuccess')
and (e.hit_start_time_num + dif.diffhour*3600-seconds_listings)<86400
and a.clsfd_site_id = 201
and a.ad_start_dt >= '2015-01-01' 
group by 1,2,3,4,5)
with data primary index (clsfd_ad_id);
collect statistics column (clsfd_ad_id) on p_alexey2_t.day_replies;




---listings table
drop table p_alexey2_t.day_listings ;
create multiset table p_alexey2_t.day_listings as (
select  cal.year_id
	,cal.month_id
	,cal.cal_dt
	,case when ad.clsfd_pltfrm_id=1 then 'site'
			when ad.clsfd_pltfrm_id=2 then 'mweb'
			when ad.clsfd_pltfrm_id=3 then 'iphone'
			when ad.clsfd_pltfrm_id=4 then 'android'
			else 'unknown' end as platform
	,ctg.clsfd_meta_categ_name 
	,ctg.clsfd_categ_type
	,loc.clsfd_lvl2_loc_name 
	,case when clsfd_proxy_id is null then 'non_api' else 'api' end as api_ind
	,ad.clsfd_ad_id 	
	,case when rp.clsfd_ad_id is not null then 1 else 0 end as day_contacted_listing
from clsfd_access_views.dw_clsfd_ad ad
left join p_alexey2_t.day_replies rp
on ad.clsfd_ad_id = rp.clsfd_ad_id
and ad.clsfd_site_id = rp.clsfd_site_id
join clsfd_access_views.clsfd_ad_sts_chng_hst hst
on ad.clsfd_ad_id = hst.clsfd_ad_id
and hst.ad_status_start_date <= ad.src_cre_dt + interval '30' day
and hst.ad_status_end_date > ad.src_cre_dt + interval '30' day
and (hst.ad_status_id in (1,2,3,12,30)) 
join access_views.dw_cal_dt cal
on ad.src_cre_dt = cal.cal_dt
left join vt_loc loc
on ad.clsfd_geo_ref_id = loc.clsfd_geo_ref_id
and ad.clsfd_site_id = loc.clsfd_site_id
left join vt_ctg ctg
on ad.clsfd_categ_ref_id = ctg.clsfd_categ_ref_id
and ad.clsfd_site_id = ctg.clsfd_site_id
where ad.src_cre_dt >= date '2016-01-01'
and ad.clsfd_site_id in (201))
with data primary index (clsfd_ad_id);
collect statistics column (clsfd_ad_id) on p_alexey2_t.day_listings;



select year_id
		,month_id
		,count(clsfd_ad_id)
		,sum(day_contacted_listing) from p_alexey2_t.day_listings
group by 1,2








