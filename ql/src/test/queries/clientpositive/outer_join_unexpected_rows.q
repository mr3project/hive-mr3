create table t_y0 (id integer,s string);
create table t_xy0 (id integer,s string);

insert into t_y0 VALUES(0,'a'),(1,'y');
insert into t_xy0 VALUES(1,'x'),(1,'y');

select * from t_xy0 l full outer join t_y0 r on (l.id=r.id and l.s='y');

create table t_y (id integer,s string);
create table t_xy (id integer,s string);

insert into t_y values(0,'a'),(1,'y'),(1,'x');
insert into t_xy values(1,'x'),(1,'y');

select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
set hive.auto.convert.join=true;
select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
set hive.cbo.enable=false;
select * from t_xy l full outer join t_y r on (l.id=r.id and l.s='y');
