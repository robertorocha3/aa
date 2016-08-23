use `mysql` ;
drop schema if exists `rep` ;
create schema if not exists `rep` default character set utf8 collate utf8_general_ci ;
grant all privileges on `rep`.* to 'repuser'@'%' identified by 'letmein' with grant option;
flush privileges;
use `rep` ;
drop table if exists `rep`.`rep_run`;
create table `rep`.`rep_run`(
    `id` int(8) unsigned not null primary key,
	`repname` varchar(10) not null,
    `startTime` bigint signed not null,
    `finishTime` bigint signed not null
)engine=InnoDB;

drop table if exists `rep`.`rep_record`;
create table `rep`.`rep_record`(
    `runId` int(8) unsigned not null,
	`recordNumber` int(10) unsigned not null,
	`fileName` varchar(256) not null,
	`url` varchar(300) not null,
	`domname` varchar(50) not null,
	`extname` varchar(50) not null,
	`numberOfRecordFields` int(10) unsigned not null,
	`numberOfFIleFields` int(10) unsigned not null,
	`analysisDuration` int(10) unsigned not null
)engine=InnoDB;

drop table if exists `rep`.`rep_download`;
create table `rep`.`rep_download`(
    `runId` int(8) unsigned not null,
	`recordNumber` int(10) unsigned not null,
	`fileName` varchar(256) not null,
	`url` varchar(300) not null,
	`fileSize` int(10) unsigned not null,
	`domname` varchar(50) not null,
	`statusCode` int(4) unsigned not null,
	`statusMessage` varchar(25) not null,
	`success` enum('y','n') not null,
    `startTime` bigint signed not null,
	`finishTime` bigint signed not null,
	`extname` varchar(50) not null,
	`existingFile` enum('y','n') not null
)engine=InnoDB;

drop table if exists `rep`.`rep_match`;
create table `rep`.`rep_match`(
    `runId` int(8) unsigned not null,
	`recordNumber` int(10) unsigned not null,
	`recordKey` varchar(50) not null,
    `fileKey` varchar(50) not null,
    `recordValue` blob not null,
	`fileValue` blob not null,
	`fileName` varchar(256) not null,
	`literal` enum('y','n') not null,
	`removeMultipleSpaces` enum('y','n') not null,
    `leaveOnlyNumbersAndLetters` enum('y','n') not null,
    `replaceNewLinesWithSpaces` enum('y','n') not null,
	`removeLeadingAndTrailingSpaces` enum('y','n') not null
)engine=InnoDB;

drop table if exists `rep`.`rep_id`;
create table `rep`.`rep_id`(
    `runId` int(8) unsigned not null,
	`idname` varchar(20) not null,
    `numFound` int(20) unsigned not null,
    `idType` varchar(10) not null
)engine=InnoDB;

describe `rep`.`rep_run`;
describe `rep`.`rep_record`;
describe `rep`.`rep_download`;
describe `rep`.`rep_match`;
describe `rep`.`rep_id`;
