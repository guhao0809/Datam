

  library(data.table)
  library(stringr)
  library(WindR)
  w.start()
  library(lubridate)
  library(DBI)
  library(RMySQL)



#持仓数据的处理
fhold<-attach(paste(adr,'fhold_dc.Rdata',sep=''),pos=2)$fhold
detach(pos=2)fhold<-as.data.table(fhold)
fhold<-fhold[order(code,date,na.last = TRUE,decreasing = FALSE),]
dbWriteTable(con, "fundhold", fhold, overwrite=FALSE, append=TRUE,row.names=F)





#td 函数生成日期序列数据
std<-min(fhold$date)
eed<-max(fhold$date)

  
wd<-data.table(date=as.Date(w.tdays(std,eed,Days='Alldays')$Data[,1]))
td<-data.table(date=as.Date(w.tdays(std,eed,Days='Trading')$Data[,1]))
ztd<-data.table(date=as.Date(w.tdays(std,eed,Days='Trading')$Data[,1]))

yl<-unique(year(td$date))
q1l<-as.Date(str_c(yl,'03','31',sep='-'))
q2l<-as.Date(str_c(yl,'06','30',sep='-'))
q3l<-as.Date(str_c(yl,'09','30',sep='-'))
q4l<-as.Date(str_c(yl,'12','31',sep='-'))
ql<-data.table(date=c(q1l,q2l,q3l,q4l))  

wd$pred<-wd$date-1

pretd<-c(as.Date(w.tdaysoffset(-1,td$date[1])$Data[,1]),td$date[-length(td$date)])
td$pretd<-pretd


ztd<-rbind(ztd,ql[ql$date>=min(td$date)& ql$date<=max(td$date)])
ztd<-unique(ztd)
ztd<-ztd[order(date)]

preztd<-c(as.Date(w.tdaysoffset(-1,ztd$date[1])$Data[,1]),ztd$date[-length(ztd$date)])
ztd$preztd<-preztd


zotd<-merge(wd,td,by='date',all.x=TRUE)
zotd<-merge(zotd,ztd,by='date',all.x=TRUE)

con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="xiangaixu",password="123456")
dbListTables(con) 
dbSendQuery(con,'SET NAMES gbk')

dbWriteTable(con, "zotd", zotd, overwrite=FALSE, append=TRUE,row.names=F)




#净资产数据的整理
 fasset<-attach(paste(adr,'fasset_dc.Rdata',sep=''),pos=2)$fasset
 detach(pos=2)
 fasset<-as.data.table(fasset)
 fasset<-fasset[order(code,date,na.last = TRUE,decreasing = FALSE),]
 fasset<-fasset[!(str_detect(name,'E') & nav==0)]
 fasset[is.na(inv)]$inv<-0
 fasset[is.na(outv)]$outv<-0
 fasset[is.na(rt_dc)]$rt_dc<-0
 fasset[is.na(div)]$div<-0
 fasset[is.na(split)]$split<-1
 fasset$snav<-as.numeric(NA)
 fasset$preunv<-as.numeric(NA)
 fasset$preunv_td<-as.numeric(NA)


temp_fa<-data.table(dbGetQuery(con,str_c("select * from fundasset where date ='",zotd[date==std]$pred,"'")))
temp_fa$date<-as.Date(temp_fa$date)
temp_fa<-rbind(fasset,temp_fa)

fasset<-merge(fasset,zotd,by='date',all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,preu=unv)],by.x=c('code','pred'), by.y=c('code','date'), all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,preu_td=unv)],by.x=c('code','pretd'),by.y=c('code','date'), all.x=TRUE)



 



#交易数据的整理
#根据运营的说法，买入和卖出证券的情况下，他们的凭证应该是：
#1买入的情况下  清算金额=成本金额+利息总额+交易费用 其中成本金额=净价金额-扣掉的利息税 
#2卖出的情况下  清算金额=成本金额+利息总额-交易费用 其中成本金额=净价金额+返还的利息税
#3兑付和付息的情况 与卖出等同
#按照数据中心的字段，对于买入类型的而言，应该有fvalue=cvalue+taccvalue+fee+tax，但数据中心数据较乱，cvalue有些是净价市值，有些加了利息税，有些加了费用，导致字段的值很不稳定，
#而且有时候fvalue 和cvalue是反过来的，因此采用下面的策略进行清洗：
#1先判断cvalue和fvalue的大小，将fvalue全部清洗为清算金额
#2根据清算金额，结合买卖方向倒算cvalue
#3根据cvalue计算fvalue

ftrade<-attach(paste(adr,'ftrade_dc.Rdata',sep=''),pos=2)$ftrade
detach(pos=2)
ftrade<-as.data.table(ftrade)
ftrade<-ftrade[order(code,date,na.last = TRUE,decreasing = FALSE),]

ftrade[is.na(tax)]$tax<-0
ftrade[is.na(taccvalue)]$taccvalue<-0
ftrade[is.na(fee)]$fee<-0
ftrade[is.na(tacc)]$tacc<-0

ftrade<-ftrade[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),taccvalue=sum(taccvalue),fee=sum(fee),tax=sum(tax),tacc=sum(tacc)),by=.(code,name,date,seccode,market,atype,pos)]


ftrade$clvalue<-ftrade$fvalue
ftrade[cvalue >0 & abs(cvalue) > abs(clvalue)]$clvalue<-ftrade[cvalue >0 & abs(cvalue) > abs(clvalue)]$cvalue
#卖出的情况比较复杂，可以分为：
#1、卖出，有应计利息的情况，这种情况下，一般应计利息>费用，因此清算金额>成本金额，找出清算金额<成本金额的，进行修正
ftrade[cvalue <0 & taccvalue!=0 & abs(cvalue) > abs(clvalue)]$clvalue<-ftrade[cvalue <0 & taccvalue!=0 & abs(cvalue) > abs(clvalue)]$cvalue
#2、卖出，无应计利息的情况，这种情况下，清算金额<成本金额，找出清算金额>成本金额的，进行修正
ftrade[cvalue <0 & taccvalue==0 & abs(cvalue) < abs(clvalue)]$clvalue<-ftrade[cvalue <0 & taccvalue==0 & abs(cvalue) < abs(clvalue)]$cvalue

#由清算金额得到净价金额
ftrade[clvalue>0]$cvalue<-ftrade[clvalue>0]$clvalue-abs(ftrade[clvalue>0]$taccvalue)-abs(ftrade[clvalue>0]$tax)-abs(ftrade[clvalue>0]$fee)
ftrade[clvalue<0]$cvalue<--(abs(ftrade[clvalue<0]$clvalue)-abs(ftrade[clvalue<0]$taccvalue)-abs(ftrade[clvalue<0]$tax)+abs(ftrade[clvalue<0]$fee))

#由净价金额得到全价金额
ftrade[clvalue>0]$fvalue<-ftrade[clvalue>0]$cvalue+ftrade[clvalue>0]$taccvalue
ftrade[clvalue<0]$fvalue<-ftrade[clvalue<0]$cvalue+ftrade[clvalue<0]$taccvalue

ftrade$cprice<-ftrade$cvalue/ftrade$volume
ftrade$fprice<-ftrade$fvalue/ftrade$volume
ftrade$clprice<-ftrade$clvalue/ftrade$volume

fundtrade<-ftrade[,.(code,name,date,seccode,market,atype,volume,fprice,cprice,clprice,cvalue,fvalue,clvalue,taccvalue,fee,ttype,tattr,pos,tax,tacc)]
  library(DBI)
  library(RMySQL)
  con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="xiangaixu",password="123456")
  dbWriteTable(con, "fundtrade", fundtrade, overwrite=FALSE, append=TRUE,row.names=F)






  #取出持仓、交易、日期、净资产数据，计算得到组合个券日收益率表

  #根据持仓和交易数据，合成全持仓数据

hnt_temp<-unique(rbind(fhold[,.(code,date,seccode,market,atype)],ftrade[,.(code,date,seccode,market,atype)]))
hnt_temp<-merge(hnt_temp,zotd,by=c('date'),allow.cartesian = TRUE)
hnt_temp<-merge(hnt_temp,fhold[,.(volume=sum(volume),cvalue=sum(volume),fvalue=sum(fvalue),ctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by=c('code','date','seccode'),all.x=TRUE)
hnt_temp<-merge(hnt_temp,fhold[,.(pvolume=sum(volume),pcvalue=sum(volume),pfvalue=sum(fvalue),pctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by.x=c('code','pred','seccode','pos'),by.y=c('code','date','seccode','pos'),all.x=TRUE)
ftrade_cl<-merge(ftrade[cvalue>0,.(bvolume=sum(volume),bcvalue=sum(cvalue),bfvalue=sum(fvalue)),by=.(code,date,seccode,pos)],ftrade[cvalue<0,.(svolume=sum(volume),scvalue=sum(cvalue),sfvalue=sum(fvalue)),by=.(code,date,seccode,pos)],by=c('code','date','seccode','pos'),all=TRUE)
hnt_temp<-merge(hnt_temp,ftrade_cl,by=c('code','date','seccode','pos'),all.x=TRUE)

#计算个券收益率数据,c代表净价，f全价，h为买入日初卖出日终的方法
hnt_temp[is.na(cvalue)]$cvalue<-0
hnt_temp[is.na(scvalue)]$scvalue<-0
hnt_temp[is.na(pcvalue)]$pcvalue<-0
hnt_temp[is.na(bcvalue)]$bcvalue<-0
hnt_temp[is.na(fvalue)]$fvalue<-0
hnt_temp[is.na(sfvalue)]$sfvalue<-0
hnt_temp[is.na(pfvalue)]$pfvalue<-0
hnt_temp[is.na(bfvalue)]$bfvalue<-0


hnt_temp$secr_c_h<-(hnt_temp$cvalue+hnt_temp$scvalue)/(hnt_temp$pcvalue+hnt_temp$bcvalue)-1
hnt_temp$secr_f_h<-(hnt_temp$fvalue+hnt_temp$sfvalue)/(hnt_temp$pfvalue+hnt_temp$bfvalue)-1
con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="xiangaixu",password="123456")
dbWriteTable(con, "hnt", hnt_temp, overwrite=FALSE, append=TRUE,row.names=F)



#业绩分解数据处理
fdec<-data.table(attach(paste(adr,'fdec_dc.Rdata',sep=''),pos=2)$fdec)
detach(pos=2)
dbWriteTable(con, "fdec", fdec, overwrite=FALSE, append=TRUE,row.names=F)




