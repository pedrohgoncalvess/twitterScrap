create table tweets(
    id serial not null primary key,
    idtweet bigint not null,
    bodytt varchar(500),
    urlimg varchar(700),
    urltt varchar(30) not null,
    typemidia char(5),
    impressions int not null default 0,
    likes int not null default 0,
    retweets int not null default 0,
    rtcomment int not null default 0,
    "comment" int not null default 0,
    created_at timestamp not null ,
    atualized_at timestamp default current_timestamp at time zone 'America/Sao_Paulo'

);

create table tweetsStandby(
    idtweet bigint not null primary key,
    created_at timestamp not null,
    scraped_at timestamp default current_timestamp at time zone 'America/Sao_Paulo'
);

create or replace function attSitu() returns trigger as $$
    BEGIN
        update tweetsstandby set scraped_at=current_timestamp at time zone 'America/Sao_Paulo' where idtweet = new.idtweet;
        return new;
    end
$$ language plpgsql;

create or replace trigger attSituTweetsStandby after insert on tweetsmemes
    for each row execute function attSitu();

