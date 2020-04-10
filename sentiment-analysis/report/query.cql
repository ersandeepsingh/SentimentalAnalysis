
Match 
    (t:Topic {name: "testTopic"}), 
    (s:Source {name: "testSource"}),
    (c:Country {name: "testCountry"})
    (tt:Tweet {id: "unique_id"})
Set 
    t.totalMentions = t.totalMentions + 1, 
    s.totalMentions = t.totalMentions + 1, 
    c.totalMentions = c.totalMentions + 1, 
    t.totalSentiment = t.totalSentiment + tt.score,
    s.totalSentiment = s.totalSentiment + tt.score,
    c.totalSentiment = c.totalSentiment + tt.score,
    t.postitiveSentiment = Case When tt.score <0 Then t.postitiveSentiment else t.postitiveSentiment + tt.score End ,
    s.postitiveSentiment = Case When tt.score <0 Then s.postitiveSentiment else s.postitiveSentiment + tt.score End ,
    c.postitiveSentiment = Case When tt.score <0 Then c.postitiveSentiment else c.postitiveSentiment + tt.score End ,

    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,
    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,
    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,

    t.negativeSentiment = Case When tt.score >0 Then t.negativeSentiment else t.negativeSentiment + tt.score End ,
    s.negativeSentiment = Case When tt.score >0 Then s.negativeSentiment else s.negativeSentiment + tt.score End ,
    c.negativeSentiment = Case When tt.score >0 Then c.negativeSentiment else c.negativeSentiment + tt.score End ,

    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,
    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,
    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,

    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End ,
    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End ,
    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End



    Match 
    (t:Topic {name: "testTopic"}), 
    (s:Source {name: "testSource"}),
    (c:Country {name: "empty"}),
    (tt:Tweet {id: 1112485274964160513})
Set 
    t.totalMentions = t.totalMentions + 1, 
    s.totalMentions = t.totalMentions + 1, 
    c.totalMentions = c.totalMentions + 1, 
    t.totalSentiment = t.totalSentiment + tt.score,
    s.totalSentiment = s.totalSentiment + tt.score,
    c.totalSentiment = c.totalSentiment + tt.score,
    t.postitiveSentiment = Case When tt.score <0 Then t.postitiveSentiment else t.postitiveSentiment + tt.score End ,
    s.postitiveSentiment = Case When tt.score <0 Then s.postitiveSentiment else s.postitiveSentiment + tt.score End ,
    c.postitiveSentiment = Case When tt.score <0 Then c.postitiveSentiment else c.postitiveSentiment + tt.score End ,

    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,
    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,
    t.postitiveMentions = Case When tt.score <0 Then t.postitiveMentions else t.postitiveMentions +1 End ,

    t.negativeSentiment = Case When tt.score >0 Then t.negativeSentiment else t.negativeSentiment + tt.score End ,
    s.negativeSentiment = Case When tt.score >0 Then s.negativeSentiment else s.negativeSentiment + tt.score End ,
    c.negativeSentiment = Case When tt.score >0 Then c.negativeSentiment else c.negativeSentiment + tt.score End ,

    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,
    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,
    t.negativeMentions = Case When tt.score >0 Then t.negativeMentions else t.negativeMentions +1 End ,

    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End ,
    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End ,
    t.neutralMentions = Case When tt.score =0 Then t.neutralMentions else t.neutralMentions +1 End
    Return t,s 
    
    Match 
    (t:Topic {name: $topicName}), 
    (s:Source {name: $sourceName}),
    (c:Country {name: $countryName}),
    (tt:Tweet {tweet_id: $tweet_id})
Set 
    t.totalMentions = t.totalMentions + 1, 
    s.totalMentions = s.totalMentions + 1, 
    c.totalMentions = c.totalMentions + 1, 
    t.totalSentiment = t.totalSentiment + tt.score,
    s.totalSentiment = s.totalSentiment + tt.score,
    c.totalSentiment = c.totalSentiment + tt.score"""
    # t.postitiveSentiment = Case When tt.score >0 Then t.postitiveSentiment+tt.score else t.postitiveSentiment End ,
    # s.postitiveSentiment = Case When tt.score >0 Then s.postitiveSentiment+tt.score else s.postitiveSentiment End ,
    # c.postitiveSentiment = Case When tt.score >0 Then c.postitiveSentiment+tt.score else c.postitiveSentiment End ,

    # t.postitiveMentions = Case When tt.score >0 Then t.postitiveMentions + 1 else t.postitiveMentions  End ,
    # t.postitiveMentions = Case When tt.score >0 Then t.postitiveMentions + 1 else t.postitiveMentions  End ,
    # t.postitiveMentions = Case When tt.score >0 Then t.postitiveMentions + 1 else t.postitiveMentions  End ,

    # t.negativeSentiment = Case When tt.score <0 Then t.negativeSentiment + tt.score else t.negativeSentiment  End ,
    # s.negativeSentiment = Case When tt.score <0 Then s.negativeSentiment + tt.score else s.negativeSentiment  End ,
    # c.negativeSentiment = Case When tt.score <0 Then c.negativeSentiment + tt.score else c.negativeSentiment  End ,

    # t.negativeMentions = Case When tt.score <0 Then t.negativeMentions +1 else t.negativeMentions  End ,
    # t.negativeMentions = Case When tt.score <0 Then t.negativeMentions +1 else t.negativeMentions  End ,
    # t.negativeMentions = Case When tt.score <0 Then t.negativeMentions +1 else t.negativeMentions  End ,

    # t.neutralMentions = Case When tt.score =0 Then t.neutralMentions+1 else t.neutralMentions End ,
    # t.neutralMentions = Case When tt.score =0 Then t.neutralMentions+1 else t.neutralMentions End ,
    # t.neutralMentions = Case When tt.score =0 Then t.neutralMentions+1 else t.neutralMentions End  

