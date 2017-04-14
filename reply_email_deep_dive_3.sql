
--visits by platform
select 	cal.week_end_dt
		, clsfd_pltfrm_desc
		, sum (clsfd_vst_cnt) as sessions
from clsfd_access_views.clsfd_pi_session as p 
left join clsfd_pltfrm_lkp pl 
on p.clsfd_pltfrm_id = pl.clsfd_pltfrm_id -- left join has better performance than inner join based on the "explain" results
left join clsfd_dvic_lkp dl 
on p.clsfd_dvic_id = dl.clsfd_dvic_id 	
join access_views.dw_cal_dt cal
on p.clsfd_sum_dt = cal.cal_dt
where p.clsfd_sum_dt > '2016-01-01' 
and p.clsfd_site_id in (201) 
group by 1,2

--replies by platform
select cal.week_end_dt
	,clsfd_pltfrm_name
	,clsfd_site_id
	,clsfd_reply_type_name	
	,sum(reply_cnt) as reply_cnt
from p_clsfd_t.clsfd_reply_all_type  rep
join access_views.dw_cal_dt cal 
on rep.clsfd_sum_dt=cal.cal_dt 
where clsfd_site_id in (201)/*with auto*/   
and clsfd_sum_dt >= date'2015-01-01'  
group by 1,2,3,4


select cal.week_end_dt
	,clsfd_site_id
	,clsfd_reply_type_name	
	,sum(reply_cnt) as reply_cnt
from p_clsfd_t.clsfd_reply_all_type  rep
join access_views.dw_cal_dt cal 
on rep.clsfd_sum_dt=cal.cal_dt 
where clsfd_site_id in (201)/*with auto*/   
and clsfd_sum_dt >= date'2015-01-01'  
group by 1,2,3



~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~`
create volatile table vt_loc as (
select  gref.clsfd_site_id
        ,clsfd_geo_ref_id
        ,lv2.clsfd_lvl2_loc_name
from clsfd_access_views.dw_clsfd_geo_ref_lkp gref
left join clsfd_access_views.dw_clsfd_lvl1_loc_lkp lv1
on gref.clsfd_lvl1_loc_id = lv1.clsfd_lvl1_loc_id
and gref.clsfd_site_id = lv1.clsfd_site_id
left join clsfd_access_views.dw_clsfd_lvl2_loc_lkp lv2
on gref.clsfd_lvl2_loc_id = lv2.clsfd_lvl2_loc_id
and gref.clsfd_site_id = lv2.clsfd_site_id
where gref.clsfd_site_id in (201)
)with data primary index(clsfd_site_id, clsfd_geo_ref_id,clsfd_lvl2_loc_name)
on commit preserve rows;



-- Prepare list for categ
create volatile table vt_ctg  as (
select  cref.clsfd_site_id
        ,meta.clsfd_meta_categ_name
        ,cref.clsfd_categ_ref_id
from clsfd_access_views.dw_clsfd_categ_ref_lkp cref
left join clsfd_access_views.dw_clsfd_meta_categ_lkp meta
on cref.clsfd_meta_categ_id = meta.clsfd_meta_categ_id
and cref.clsfd_categ_file_id = meta.clsfd_categ_file_id
and cref.clsfd_site_id = meta.clsfd_site_id
where cref.clsfd_site_id in (201)
)with data primary index(clsfd_site_id, clsfd_meta_categ_name,clsfd_categ_ref_id)
on commit preserve rows;

create volatile table reply  as (
select  rp.clsfd_site_id
        ,cal.week_end_dt as clsfd_sum_dt --need to change the date to week_end_dt for weekly report
        ,ad.clsfd_pltfrm_id as platform
        ,ctg.clsfd_meta_categ_name as category_l1
        ,loc.clsfd_lvl2_loc_name as location_l2
        ,count(distinct rp.clsfd_reply_id) as rply
from clsfd_access_views.dw_clsfd_reply rp
join clsfd_access_views.dw_clsfd_ad ad
on ad.clsfd_ad_id = rp.clsfd_ad_id
and ad.clsfd_site_id = rp.clsfd_site_id
join  clsfd_access_views.clsfd_ad_sts_chng_hst hst
on ad.clsfd_ad_id = hst.clsfd_ad_id
and hst.ad_status_start_date <= ad.src_cre_dt + interval '7' day
and hst.ad_status_end_date > ad.src_cre_dt + interval '7' day
and (hst.ad_status_id in (1,2,3,12))  --only include the user posted valid organic ad( active, expired, user deleted)
join access_views.dw_cal_dt cal
on rp.src_cre_dt  = cal.cal_dt
left join vt_loc loc
on ad.clsfd_geo_ref_id = loc.clsfd_geo_ref_id
and ad.clsfd_site_id = loc.clsfd_site_id
left join vt_ctg ctg
on ad.clsfd_categ_ref_id = ctg.clsfd_categ_ref_id
and ad.clsfd_site_id = ctg.clsfd_site_id
where rp.reply_status_id in (2,3,6,14,16,20,-99) --valid reply
and rp.src_cre_dt >= date '2015-01-01'
and rp.clsfd_site_id in (201)
group by 1,2,3,4,5
)with data primary index(clsfd_site_id, clsfd_sum_dt)
on commit preserve rows;


create multiset volatile table ads  as (
select  ad.clsfd_site_id
        ,cal.week_end_dt as clsfd_sum_dt --need to change the date to week_end_dt for weekly report
        ,ad.clsfd_pltfrm_id as platform
        ,ctg.clsfd_meta_categ_name as category_l1
        ,loc.clsfd_lvl2_loc_name as location_l2
        ,count(distinct ad.clsfd_ad_id) as c2c_ad_cnt   
from clsfd_access_views.dw_clsfd_ad ad
join  clsfd_access_views.clsfd_ad_sts_chng_hst hst
on ad.clsfd_ad_id = hst.clsfd_ad_id
and hst.ad_status_start_date <= ad.src_cre_dt + interval '7' day
and hst.ad_status_end_date > ad.src_cre_dt + interval '7' day
and (hst.ad_status_id in (1,2,3,12))  
join access_views.dw_cal_dt cal
on ad.src_cre_dt = cal.cal_dt
left join vt_loc loc
on ad.clsfd_geo_ref_id = loc.clsfd_geo_ref_id
and ad.clsfd_site_id = loc.clsfd_site_id
left join vt_ctg ctg
on ad.clsfd_categ_ref_id = ctg.clsfd_categ_ref_id
and ad.clsfd_site_id = ctg.clsfd_site_id
where ad.src_cre_dt >= date '2015-01-01'
and ad.clsfd_site_id in (201)
group by 1,2,3,4,5
)with data primary index(clsfd_site_id, clsfd_sum_dt)
on commit preserve rows;

select * from reply r
left join ads a 
on a.clsfd_site_id=r.clsfd_site_id
and a.clsfd_sum_dt=r.clsfd_sum_dt
and a.platform=r.platform
and a.category_l1=r.category_l1
and a.location_l2=r.location_l2






