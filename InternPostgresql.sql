
--Task 1: Create foreign table topups

CREATE FOREIGN TABLE topups
     (seq varchar, 
	  id_user varchar, 
	  topup_date varchar,
	  topup_val varchar)
   SERVER local_server
     OPTIONS (filename 'C:/Program Files/PostgreSQL/12/data/topups.tsv', DELIMITER E'\t' );

    
--Task 2:Print out the list of user IDs and the total of all top-ups done by them but ONLY for the users that had at least one 
--topup ever done by the amount of €15 exactly. 

select t.id_user,SUM(cast(topup_val as integer)) as total_topup_val
from topups as t inner join (Select id_user, COUNT(*)  as topup_15_eur
from topups as t
where cast(topup_val as integer) = 15
and topup_val <> 'topup_val' 
group by id_user
having COUNT(*) >= 1 ) t_15 on t_15.id_user = t.id_user
where topup_val <> 'topup_val'
group by t.id_user;


--Tesk 3: Show the 5 (but not more) rows containing most recent top-ups per user. 
select *
from
(
select id_user, topup_date , topup_val ,
dense_rank() over (partition by id_user order by topup_date desc) as DRank_Seq,
rank() over (partition by id_user order by topup_date desc) as Rank_Seq,
row_number() over (partition by id_user order by topup_date desc) as RN_Seq
from topups 
where topup_val <> 'topup_val'
) as A
where A.Rank_Seq <= 5;

--Task 4: Show the 5 largest top ups done per user. 
Select * from
(
select id_user, topup_date , topup_val
, row_number() over (partition by id_user order by cast(topup_val as integer) desc) as rownumber
from topups 
where topup_val <> 'topup_val'
) as A
where A.rownumber <= 5;

--Task 5
--prv_topup_dt - previous topup date of the same user
--days_since - number of days since previous topup by user
--promo_ind - Y/N flag. Put Y for top-ups of €20 or more, otherwise N.
--previous_qual_topup_dt - the date of previous topup of €20 or more done by the same user
--to_1st_ratio - (bonus) Y/X fraction value where Y is the current topup value and X is the amount of the first ever topup done by the user.


Select id_user,topup_date,cast(topup_val as integer) as topup_val,
LAG(topup_date,1) OVER (PARTITION BY id_user ORDER by topup_date) AS prev_topup_date, 
CAST(topup_date as date) - cast(LAG(topup_date,1) OVER (PARTITION BY id_user ORDER by topup_date) as date) as days_since,
case when cast(topup_val as integer) >= 20
     then 'Y'
     else 'N' end as promo_ind,
CASE WHEN LAG(cast(topup_val as integer),1)  OVER (PARTITION BY id_user ORDER BY cast(topup_date as date)) >= 20
     then LAG(cast(topup_date as date),1) OVER (PARTITION BY id_user ORDER BY cast(topup_date as date))
	 else Null END as previous_qual_topup_dt,
cast(topup_val as integer)/first_value(cast(topup_val as integer))OVER (PARTITION BY id_user ORDER BY cast(topup_date as date))::float as to_1st_ratio 
into table_derived
from topups 
where topup_val <> 'topup_val'


--Task 6 - Print out the list of consolidated periods when users were eligible to make free calls. Include initial eligibility date and the date when the free credit effectively ends. 
Select *, Row_number() over (partition by id_user,consecutive_topups order by promo_start)
from
(
Select id_user, topup_date as promo_start, 
CASE WHEN LEAD(cast(topup_date as date),1) OVER (PARTITION by id_user ORDER by cast(topup_date as date)) <= cast(topup_date as date) + interval '28' day
     THEN cast(topup_date as date) + interval '28' day
	 ELSE cast(topup_date as date) + interval '28' day END as promo_end,
CASE WHEN LEAD(cast(topup_date as date),1) OVER (PARTITION by id_user ORDER by cast(topup_date as date)) <= cast(topup_date as date) + interval '28' day 
     THEN 1
	 ELSE 0 END as consecutive_topups
FROM topups as t 
WHERE cast(topup_val as integer) >= 20
and topup_val <> 'topup_val') as top20 
order by id_user, promo_start

--
select * from table_derived