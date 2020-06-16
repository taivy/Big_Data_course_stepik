**Задания**:

Представьте, что вы устроились работать аналитиком в отдел рекламы, и ваша первая задача помочь коллегам разобраться с некоторыми вопросами:

1. Получить статистику по дням. Просто посчитать число всех событий по дням, число показов, число кликов, число уникальных объявлений и уникальных кампаний.
1. Разобраться, почему случился такой скачок 2019-04-05? Каких событий стало больше? У всех объявлений или только у некоторых?
1. Найти топ 10 объявлений по CTR за все время. CTR — это отношение всех кликов объявлений к просмотрам. Например, если у объявления было 100 показов и 2 клика, CTR = 0.02. Различается ли средний и медианный CTR объявлений в наших данных?
1. Похоже, в наших логах есть баг, объявления приходят с кликами, но без показов! Сколько таких объявлений, есть ли какие-то закономерности? Эта проблема наблюдается на всех платформах?
1. Есть ли различия в CTR у объявлений с видео и без? А чему равняется 95 процентиль CTR по всем объявлениям за 2019-04-04?
1. Для финансового отчета нужно рассчитать наш заработок по дням. В какой день мы заработали больше всего? В какой меньше? Мы списываем с клиентов деньги, если произошел клик по CPC объявлению, и мы списываем деньги за каждый показ CPM объявления, если у CPM объявления цена - 200 рублей, то за один показ мы зарабатываем 200 / 1000.
1. Какая платформа самая популярная для размещения рекламных объявлений? Сколько процентов показов приходится на каждую из платформ (колонка platform)?
1. А есть ли такие объявления, по которым сначала произошел клик, а только потом показ?


**Решения:**

1.
```
select date, count(event) count_events, countIf(event='view') count_views, countIf(event='click') count_clicks, count(distinct ad_id) count_ads, count(distinct campaign_union_id) count_campaigns from ads_data group by date;
```

2.

Было запущены популярное объявление 112583

```
select date, ad_id, count(ad_id) cnt from ads_data
group by ad_id, date
order by cnt desc, date desc
limit 5;
```

Видно, что за все дни больше всего показов было у объявления 112583 2019-04-05 393828 показов. У второго объявления по событиям количество более чем в 2 раза меньше - 154968


3.

Топ 10 объявлений по CTR:
```
select ad_id,
countIf(event = 'view') as views,
countIf(event = 'click') as clicks,
clicks/views as ctr
from ads_data
group by ad_id
having views != 0
order by ctr desc
limit 10;
```

Средний CTR - 0.20411823080602942

```
select avg(ctr) avg_str from (
    select ad_id,
    countIf(event = 'view')  as views,
    countIf(event = 'click') as clicks,
    clicks / views as ctr
    from ads_data
    group by ad_id
    having views != 0
    order by ctr desc
    limit 10
);
```

Медианный - 0.17028985507246375

```
select median(ctr) median_str from (
    select ad_id,
    countIf(event = 'view')  as views,
    countIf(event = 'click') as clicks,
    clicks / views as ctr
    from ads_data
    group by ad_id
    having views != 0
    order by ctr desc
    limit 10
);
```

4.

Всего объявлений без показов 36.

```
select count(ad_id) counts from (
    select date, platform, ad_id,
    countIf(event = 'view') as views,
    countIf(event = 'click') as clicks
    from ads_data
    group by ad_id, platform, date
    having (views = 0 and clicks != 0)
    order by date, platform
);
```

Чаще всего такой баг появляется на ios и android.

```
select platform, count(ad_id) counts from (
    select date, platform, ad_id,
    countIf(event = 'view') as views,
    countIf(event = 'click') as clicks
    from ads_data
    group by ad_id, platform, date
    having (views = 0 and clicks != 0)
    order by date, platform
)
group by platform
order by counts desc;
```

Чаще всего баг случался 2019-04-01.

```
select date, count(ad_id) counts from (
    select date, platform, ad_id,
    countIf(event = 'view') as views,
    countIf(event = 'click') as clicks
    from ads_data
    group by ad_id, platform, date
    having (views = 0 and clicks != 0)
    order by date, platform
)
group by date
order by counts desc;
```

5.
Для объявлений с видео средний CTR: 0.009760564638613418, медианный -  0.020144819007647034

```
select median(ctr) median_ctr_has_video, avg(ctr) avg_ctr_has_video from (
    select ad_id,
    countIf(event = 'view')  as views,
    countIf(event = 'click') as clicks,
    clicks / views as ctr
    from ads_data
    where has_video = 1
    group by ad_id
    having views != 0
    order by ctr desc
);
```

Для объявлений без видео средний CTR: 0.015745122787342112, медианный - 0.0027327504461252435

```
select median(ctr) median_ctr_not_has_video, avg(ctr) avg_ctr_not_has_video from (
    select ad_id,
    countIf(event = 'view')  as views,
    countIf(event = 'click') as clicks,
    clicks / views as ctr
    from ads_data
    where has_video = 0
    group by ad_id
    having views != 0
    order by ctr desc
);
```

Выводы: разницы в CTR есть, у рекламы с видео меньше средний, но больше медианный CTR


95 квантиль - 0.06557971014492753

```
select quantile(0.95)(ctr) quantile_95 from (
    select ad_id,
    countIf(event = 'view')  as views,
    countIf(event = 'click') as clicks,
    clicks / views as ctr
    from ads_data
    group by ad_id
    having views != 0
    order by ctr desc
);
```
 

6.

```
select date, sum(multiIf((ad_cost_type = 'CPC') AND (event = 'click'), ad_cost, (ad_cost_type = 'CPM') AND (event = 'view'), ad_cost / 1000, 0)) income
from ads_data
group by date;
```

Больше всего заработали 2019-04-05 - 96123, меньше - 2019-04-01 - 6656


7.

Самая популярная платформа - android

```
select (select count(ad_id) from ads_data where event='view') views_total, platform, count(ad_id) / views_total popularity_percent
from ads_data
where event='view'
group by platform
order by popularity_percent desc;
```

проценты:
- android,0.5003491157604999
- ios,0.29985451639047983
- web,0.19979636784902036

 

8.

Да, объявления, по которым сначала произошел клик, а только потом показ есть

```
select (clicks.earliest_click - views.earliest_view) view_click_diff from
(
    select ad_id, min(time) earliest_click
    from ads_data
    where event='click'
    group by ad_id
) clicks
join
(
    select ad_id, min(time) earliest_view
    from ads_data
    where event='view'
    group by ad_id
) views
using ad_id
where view_click_diff < 0
limit 10;
```
