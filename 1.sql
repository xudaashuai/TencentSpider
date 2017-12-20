create table bj_tencentpois
(
	id text not null
		constraint tencentpois_copy_pkey
			primary key,
	title text,
	address text,
	tel text,
	category text,
	type text,
	lat text,
	lng text,
	adcode text,
	boundary text,
	panoid text,
	heading text,
	pitch text,
	zoom text,
	pic integer
)
;

