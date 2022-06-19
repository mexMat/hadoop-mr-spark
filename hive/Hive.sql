with answerTime as (
    select u.displayName
         , u.id as userId
         , unix_timestamp(t2.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") - t1.questionUnixTimestamp  as speed                                                    
    from  (select id                                                        
         , unix_timestamp(creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") as questionUnixTimestamp
         , acceptedAnswerId
    from posts 
    where posttypeid = 1
      and acceptedAnswerId is not NULL
      and creationdate is not NULL) as t1
             join posts as t2
                  on t1.acceptedanswerid = t2.id
             join users as u
                  on t2.owneruserid = u.id
    where  t2.owneruserid is not NULL
      and t2.posttypeid = 2
      and t2.creationdate is not null
      and unix_timestamp(t2.creationdate, "yyyy-MM-dd'T'HH:mm:ss.SSS") -
          t1.questionUnixTimestamp >= 5
)

select userId, displayName
       , avg(speed) as answerSpeed
from answerTime
group by userId, displayName
order by answerSpeed asc
