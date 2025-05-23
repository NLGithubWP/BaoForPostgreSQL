create table users(id integer not null primary key, reputation integer not null, creationdate bigint not null, views integer not null, upvotes integer not null, downvotes integer not null);
create table posts(id integer not null primary key, posttypeid smallint not null, creationdate bigint not null, score integer not null, viewcount integer not null, owneruserid integer not null, answercount integer not null, commentcount integer not null, favoritecount integer not null, lasteditoruserid integer not null);
create table postlinks(id integer not null primary key, creationdate bigint not null, postid integer not null, relatedpostid integer not null, linktypeid smallint not null);
create table posthistory(id integer not null primary key, posthistorytypeid smallint not null, postid integer not null, creationdate bigint not null, userid integer not null);
create table comments(id integer not null primary key, postid integer not null, score smallint not null, creationdate bigint not null, userid integer not null);
create table votes(id integer not null primary key, postid integer not null, votetypeid smallint not null, creationdate bigint not null, userid integer not null, bountyamount smallint not null);
create table badges(id integer not null primary key, userid integer not null, date bigint not null);
create table tags(id integer not null primary key, count integer not null, excerptpostid integer not null);
create index idx_posts_owneruserid on posts using btree(owneruserid);
create index idx_posts_lasteditoruserid on posts using btree(lasteditoruserid);
create index idx_postlinks_relatedpostid on postlinks using btree(relatedpostid);
create index idx_postlinks_postid on postlinks using btree(postid);
create index idx_posthistory_postid on posthistory using btree(postid);
create index idx_posthistory_userid on posthistory using btree(userid);
create index idx_comments_postid on comments using btree(postid);
create index idx_comments_userid on comments using btree(userid);
create index idx_votes_userid on votes using btree(userid);
create index idx_votes_postid on votes using btree(postid);
create index idx_badges_userid on badges using btree(userid);
create index idx_tags_excerptpostid on tags using btree(excerptpostid);

