SELECT
      REVIEW_ID               ,
      ORDER_ID                ,
      REVIEW_SCORE            ,
      REVIEW_COMMENT_TITLE    ,
      REVIEW_COMMENT_MESSAGE  ,
      REVIEW_CREATION_DATE    ,
      REVIEW_ANSWER_TIMESTAMP ,
      INSERTED_AT             
FROM 
      {{ source('bronze', 'order_reviews') }}
