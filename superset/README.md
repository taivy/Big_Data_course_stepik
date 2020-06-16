**Задания**:

Для финального задания нужно построить дашборд по данным flights, в которых представлена выборка данных об авиоперелетах в США за январь 2013 года. Эта таблица уже добавлена в Superset и вы можете начинать с ней работать. 

Создайте дашборд. Добавьте на дашборд серию графиков, которые отвечают на следующие вопросы:

 -   Сколько перелетов совершается по дням, по неделям?
 -   Сколько перелетов совершается по дням, по неделям без задержек в отправлении, а сколько отправляется раньше назначенного времени?
 -   Какие самые популярные направления за этот месяц, январь 2013 года?
 -   Какие воздушные судна совершили наибольшее число перелетов?



 Описание колонок в данных

-   year, month, day - Date of departure.
-   dep_time, arr_time - Actual departure and arrival times (format HHMM or HMM), local tz.
-   sched_dep_time, sched_arr_time - Scheduled departure and arrival times (format HHMM or HMM), local tz.
-   dep_delay - Departure delays, in minutes. Negative times represent early departures/arrivals.
-   carrier - Two letter carrier abbreviation.  
-   flight - Flight number.
-   tailnum - Plane tail number.  
-   origin, dest - Origin and destination.  
-   air_time - Amount of time spent in the air, in minutes.
-   distance - Distance between airports, in miles.
-   hour, minute - Time of scheduled departure broken into hour and minutes.
-   time_hour - Scheduled date and hour of the flight as a POSIXct date.  



**Решение**:

Скриншот получившихся графиков в файле result.png
