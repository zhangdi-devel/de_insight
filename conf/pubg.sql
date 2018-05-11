DROP TABLE IF EXISTS pubg;
CREATE TYPE my_period AS ENUM('lastHour', 'lastDay', 'lastMonth');
CREATE TYPE my_tag AS ENUM('C', 'R', 'N');
CREATE TABLE pubg (
	player VARCHAR(256),
	period my_period,
	time TIMESTAMP(3),
	kills INT,
	deaths INT,
	reports INT,
	reported INT,
	tag my_tag,
	CONSTRAINT player_period PRIMARY KEY (player, period)
);
CREATE INDEX ON pubg (kills);
CREATE INDEX ON pubg (deaths);
CREATE INDEX ON pubg (reports);
CREATE INDEX ON pubg (reported);
