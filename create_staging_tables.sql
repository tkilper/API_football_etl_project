-- Create Football API Staging Tables
create table "Staging".dim_fixture_events (
	type varchar(255),
	detail varchar(255),
	comments varchar(255),
	"time.elapsed" integer,
	"time.extra" decimal,
	"team.id" integer,
	"player.id" integer,
	"assist.id" decimal,
	"parameters.fixture" varchar(255),
	load_timestamp date
)

create table "Staging".dim_fixture_lineups (
	"player.id" integer,
	"player.number" integer,
	"player.pos" varchar(5),
	"player.grid" varchar(5),
	formation varchar(255),
	"team.id" integer,
	"coach.id" integer,
	"parameters.fixture" integer,
	status varchar(10),
	load_timestamp date
)

create table "Staging".dim_fixture_stats (
	type varchar(255),
	value integer,
	"team.id" integer,
	"parameters.fixture" integer,
	load_timestamp timestamp,
)

create table "Staging".fact_fixture (
	"fixture.id" integer,
	"fixture.referee" varchar(50),
	"fixture.timezone" varchar(5),
	"fixture.date" varchar(50),
	"fixture.timestamp" varchar(50),
	"fixture.periods.first" integer,
	"fixture.periods.second" integer,
	"fixture.venue.id" integer,
	"fixture.status.long" varchar(50),
	"fixture.status.short" varchar(50),
	"fixture.status.elapsed" varchar(50),
	"league.id" integer,
	"league.name" varchar(255),
	"league.country" varchar(255),
	"league.logo" varchar(255),
	"league.flag" varchar(255),
	"league.season" varchar(255),
	"league.round" varchar(255),
	"teams.home.id" integer,
	"teams.home.winner" varchar(255),
	"teams.away.id" integer,
	"teams.away.winner" varchar(255),
	"goals.home" varchar(255),
	"goals.away" varchar(255),
	"score.halftime.home" varchar(255),
	"score.halftime.away" varchar(255),
	"score.fulltime.home" varchar(255),
	"score.fulltime.away" varchar(255),
	"score.extratime.home" varchar(255),
	"score.extratime.away" varchar(255),
	"score.penalty.home" varchar(255),
	"score.penalty.away" varchar(255),
	load_timestamp timestamp,
	primary key ("fixture_id")
)

create table "Staging".dim_player_info (
	"player.id" integer,
	"player.name" varchar(255),
	"player.firstname" varchar(255),
	"player.lastname" varchar(255),
	"player.age" integer,
	"player.birth.date" varchar(255),
	"player.birth.place" varchar(255),
	"player.birth.country" varchar(255),
	"player.nationality" varchar(255),
	"player.height" varchar(255),
	"player.weight" varchar(255),
	"player.injured" boolean,
	"player.photo" varchar(255),
	load_timestamp timestamp,
	primary key ("player.id")
)
	
create table "Staging".dim_player_stats (
	"team.id" integer,
	"league.id" integer,
	"league.name" varchar(255),
	"league.country" varchar(255),
	"league.logo" varchar(255),
	"league.flag" varchar(255),
	"league.season" varchar(10),
	"games.appearences" integer,
	"games.lineups" varchar(255),
	"games.minutes" integer,
	"games.number" integer,
	"games.position" varchar(255),
	"games.rating" varchar(255),
	"games.captain" varchar(255),
	"substitutes.in" varchar(255),
	"substitutes.out" varchar(255),
	"substitutes.bench" varchar(255),
	"shots.total" integer,
	"shots.on" integer,
	"goals.total" integer,
	"goals.conceded" integer,
	"goals.assists" integer,
	"goals.saves" integer,
	"passes.total" integer,
	"passes.key" integer,
	"passes.accuracy" decimal,
	"tackles.total" integer,
	"tackles.blocks" integer,
	"tackles.interceptions" integer,
	"duels.total" integer,
	"duels.won" integer,
	"dribbles.attempts" integer,
	"dribbles.success" integer,
	"dribbles.past" integer,
	"fouls.drawn" integer,
	"fouls.committed" integer,
	"cards.yellow" integer,
	"cards.yellowred" integer,
	"cards.red" integer,
	"penalty.won" integer,
	"penalty.commited" integer,
	"penalty.scored" integer,
	"penalty.missed" integer,
	"penalty.saved" integer,
	"player.id" integer,
	load_timestamp timestamp,
	primary key ("player.id")
)

create table "Staging".dim_teams (
	"team.id" integer,
	"team.name" varchar(255),
	"team.code" varchar(255),
	"team.country" varchar(255),
	"team.founded" varchar(255),
	"team.national" varchar(255),
	"team.logo" varchar(255),
	"venue.id" integer,
	primary key ("team.id")
)

create table "Staging".dim_venues (
	id integer,
	name varchar(255),
	address varchar(255),
	city varchar(255),
	country varchar(255),
	capacity integer,
	surface varchar(255),
	image varchar(255),
	primary key (id)
)