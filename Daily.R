

library(data.table)
library(stringr)
library(xlsx)
library(WindR)
w.start()
library(lubridate)
library(DBI)
library(RMySQL)




adr='Z:\\Risk Control\\rawdata\\'
dbadr<-'//172.16.23.200/共享/riskplantform/db/'
#td 函数生成日期序列数据

#读取基金列表信息
fcode_dc<-as.data.table(read.xlsx2(paste(dbadr,'portfoliobasic_dc.xlsx',sep=''),1,stringsAsFactors = FALSE)[c('dccode','组合简称','组合成立日','shareopdate','数据中心代码','组合代码','账户组','内部分类','lvl','策略组')])
names(fcode_dc)<-c('code','name','setupdate','sopdate','zcode','mcode','issuetype','innerclass','lvl','dept')
fcode_dc$setupdate<-as.Date(as.numeric(fcode_dc$setupdate),origin='1899-12-30')
fcode_dc$sopdate<-as.Date(as.numeric(fcode_dc$sopdate),origin='1899-12-30')
save(fcode_dc,file=paste(adr,'fcode_dc.Rdata',sep=""))

con <- dbConnect(MySQL(),host="172.16.22.186",port=3306,dbname="rm",user="xiangaixu",password="123456")
dbSendQuery(con,'SET NAMES gbk')

dbWriteTable(con, "fundbasic", fcode_dc, overwrite=FALSE, append=TRUE,row.names=F)




fasset<-data.table(attach(paste(adr,'fasset_dc.Rdata',sep=''),pos=2)$fasset)
detach(pos=2)
eed<- max(fasset$date)
std<- min(fasset$date)

  
wd<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Alldays')$Data[,1]))
td<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Trading')$Data[,1]))
ztd<-data.table(date=as.Date(w.tdays(as.Date(w.tdaysoffset(-1,std)$Data[,1]),eed,Days='Trading')$Data[,1]))

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


dbWriteTable(con, "zotd", zotd, overwrite=FALSE, append=TRUE,row.names=F)




#净资产数据的整理
fasset<-data.table(attach(paste(adr,'fasset_dc.Rdata',sep=''),pos=2)$fasset)
detach(pos=2)
fasset<-fasset[order(code,date,na.last = TRUE,decreasing = FALSE),]
fasset<-fasset[unv!=0]
fasset[is.na(inv)]$inv<-0
fasset[is.na(outv)]$outv<-0
fasset[is.na(rt_dc)]$rt_dc<-0
fasset[is.na(div)]$div<-0
fasset[is.na(split)]$split<-1
fasset$snav<-as.numeric(NA)
fasset$preunv<-as.numeric(NA)
fasset$preunv_td<-as.numeric(NA)
fasset$preunv_ztd<-as.numeric(NA)

temp_fa<-data.table(dbGetQuery(con,str_c("select * from rm.fundasset where date in ('",unique(c(zotd[date==std]$pred,zotd[date==std]$pretd)),"')")))
temp_fa$date<-as.Date(temp_fa$date)
temp_fa<-rbind(fasset,temp_fa)

fasset<-merge(fasset,zotd,by='date',all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,preu=unv)],by.x=c('code','pred'), by.y=c('code','date'), all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,preu_td=unv)],by.x=c('code','pretd'),by.y=c('code','date'), all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,preu_ztd=unv)],by.x=c('code','preztd'),by.y=c('code','date'), all.x=TRUE)
fasset<-merge(fasset,temp_fa[,.(code,date,prenav=nav)],by.x=c('code','pred'),by.y=c('code','date'), all.x=TRUE)
fasset$preunv<-fasset$preu
fasset$preunv_td<-fasset$preu_td
fasset$preunv_ztd<-fasset$preu_ztd
fasset$snav<-fasset$prenav


#成立首日的基金，其前一日单位净值为1，snav为其inv
if (length(fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv)!=0)
{
 fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv<-1
 fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv_td<-1
 fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$preunv_ztd<-1
 fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$snav<-fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$setupdate)]$inv
}

#成立首日的A份额，其前一日单位净值为母基金前一日单位净值，snav为其份额
#成立首日的非A份额，其前一日单位净值为A份额前一日单位净值，snav为其份额
temp<-merge(fasset,fcode_dc[,.(code,sopdate)],by.x='code',by.y='code',all.x=TRUE)
temp<-temp[order(code,date,na.last = TRUE,decreasing = FALSE),]
tt<-temp[date==sopdate]
for (ii in 1:length(tt$code))
{
  if(str_detect(tt$name[ii],'A'))
  {      
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv_td<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv_td      
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii]) ]$preunv_ztd<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'A','')]$preunv_ztd
  } 
    
  if(str_detect(tt$name[ii],'[BCDE]') )
  {      
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv_td<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv_td      
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv
    fasset[str_c(code,date) == str_c(tt$code[ii],tt$date[ii])]$preunv_ztd<-temp[date==tt$date[ii] & name==str_replace(tt$name[ii],'[BCDE]','A')]$preunv_ztd   
  } 
}  
fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$sopdate)]$snav<-fasset[str_c(code,date) %in% str_c(fcode_dc$code,fcode_dc$sopdate)]$inv
#筛选一下，去掉早于份额起始日的数据
tpcd<-temp[date>=sopdate]
fasset<-fasset[str_c(code,date) %in% str_c(tpcd$code,tpcd$date)]
fasset[is.na(preunv) & !(pred %in% td$date) & date %in% td$date]$preunv<-fasset[is.na(preunv) & !(pred %in% td$date) & date %in% td$date]$preunv_td

dbWriteTable(con, "fundasset", fasset[,.(code,date,name,innercode,nav,share,unv,accunv,preunv_dc,tav,treturn,rt_dc,inv,outv,div,split,snav,preunv,preunv_td,preunv_ztd)], overwrite=FALSE, append=TRUE,row.names=F)



#持仓数据的整理
fhold<-data.table(attach(paste(adr,'fhold_dc.Rdata',sep=''),pos=2)$fhold)
detach(pos=2)
fhold<-fhold[,.(volume=sum(volume),ctvalue=sum(ctvalue),cvalue=sum(cvalue),taccvalue=sum(taccvalue),fvalue=sum(fvalue),ctprice=sum(ctvalue)/sum(volume),cprice=sum(cvalue)/sum(volume),fprice=sum(fprice)/sum(volume),tacc=sum(taccvalue)/sum(volume)),by=.(code,name,date,seccode,secname,market,pos,atype,tradeable)]
fhold<-fhold[order(code,seccode,date,na.last = TRUE,decreasing = FALSE),]
fhold<-merge(fhold,fasset[,.(code,date,nav)],by=c('code','date'),all.x=TRUE)
fhold$cratio<-fhold$cvalue/fhold$nav
fhold<-fhold[,.(code,date,name,seccode,secname,market,volume,ctvalue,cvalue,taccvalue,pos,atype,tradeable,fvalue,ctprice,cprice,fprice,tacc,nav,cratio)]
dbWriteTable(con, "fundhold", fhold, overwrite=FALSE, append=TRUE,row.names=F)



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

fundtrade<-ftrade[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),clvalue=sum(clvalue),taccvalue=sum(taccvalue),fee=sum(fee),tax=sum(tax),tacc=sum(taccvalue)/sum(volume),fprice=sum(fvalue)/sum(volume),cprice=sum(cvalue)/sum(volume),clprice=sum(clvalue)/sum(volume)),by=.(code,date,seccode,market,atype,pos,ttype,tattr)]
fundtrade<-fundtrade[,.(code,date,seccode,market,atype,volume,fprice,cprice,clprice,cvalue,fvalue,clvalue,taccvalue,fee,ttype,tattr,pos,tax,tacc)]
library(DBI)
library(RMySQL)
dbWriteTable(con, "fundtrade", fundtrade, overwrite=FALSE, append=TRUE,row.names=F)
ftrade<-ftrade[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),taccvalue=sum(taccvalue),fee=sum(fee),tax=sum(tax),tacc=sum(tacc)),by=.(code,date,seccode,market,atype,pos)]







  #取出持仓、交易、日期、净资产数据，计算得到组合个券日收益率表

  #根据持仓和交易数据，合成全持仓数据

hnt_temp<-unique(rbind(fhold[,.(code,date,seccode,market,atype)],ftrade[,.(code,date,seccode,market,atype)]))
hnt_temp<-merge(hnt_temp,zotd,by=c('date'),allow.cartesian = TRUE)
hnt_temp<-merge(hnt_temp,fhold[,.(volume=sum(volume),cvalue=sum(cvalue),fvalue=sum(fvalue),ctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by=c('code','date','seccode'),all.x=TRUE)
hnt_temp<-merge(hnt_temp,fhold[,.(pvolume=sum(volume),pcvalue=sum(cvalue),pfvalue=sum(fvalue),pctvalue=sum(ctvalue)),by=.(code,date,seccode,pos)],by.x=c('code','pred','seccode','pos'),by.y=c('code','date','seccode','pos'),all.x=TRUE)
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


hnt_temp$secr_c_h<-(hnt_temp$cvalue-hnt_temp$scvalue)/(hnt_temp$pcvalue+hnt_temp$bcvalue)-1
hnt_temp$secr_f_h<-(hnt_temp$fvalue-hnt_temp$sfvalue)/(hnt_temp$pfvalue+hnt_temp$bfvalue)-1

dbWriteTable(con, "hnt", hnt_temp, overwrite=FALSE, append=TRUE,row.names=F)



#业绩分解数据处理
fdec<-data.table(attach(paste(adr,'fdec_dc.Rdata',sep=''),pos=2)$fdec)
detach(pos=2)
fdec<-fdec[,.(gain=sum(gain),rctr=sum(rctr)),by=.(code,date,seccode,market,atype,type)]
dbWriteTable(con, "fdec", fdec, overwrite=FALSE, append=TRUE,row.names=F)



#存储市场指数数据
mindex<-data.table(attach(paste(adr,'indexprice.Rdata',sep=''),pos=2)$indexprice)
detach(pos=2)
names(mindex)[1]<-'indcode_dc'
pbench<-unique(data.table(read.xlsx2(paste(dbadr,'portbench.xlsx',sep=''),1,stringsAsFactors = FALSE))[,.(indcode,indcode_dc)])
mindex<-merge(mindex,pbench[,.(indcode,indcode_dc)],by='indcode_dc',all.x=TRUE)

pmi<-data.table(dbGetQuery(con, str_c('SELECT indcode,date,close FROM rm.mindex where date between \'', as.Date(w.tdaysoffset(-1,std)$Data[,1]), '\' and \'', std-1, '\'')))
pmi$date<-as.Date(pmi$date)

temp<-rbind(mindex[,.(indcode,date,close)],pmi[date<min(mindex$date)])
temp<-temp[order(indcode,date)]
#得到preclose和涨跌幅的数字
mi_aft<-temp[-1,]
mi_pre<-temp[-length(temp$date),]
mi_aft$preclose<-mi_pre$close
mi<-mi_aft[indcode==mi_pre$indcode]
mi$rt<-mi$close/mi$pre-1

#对于定存类的基准，获取其收益率值
indcode_cash<-c('DEPO1Y.WI','DEPO3Y.WI','DEMAND.WI','CALLDEPO7D.WI')
icode<-c('M0009808','M0009810','M0009805','M0041348')

library(WindR)
w.start()
cash_int<-as.data.table(w.edb(icode,'2000-01-01',eed,'Fill=Previous')$Data)
cash_int$DATETIME<-as.Date(cash_int$DATETIME)
sde<-max(cash_int[DATETIME<=std]$DATETIME)
cash_int<-cash_int[DATETIME>=sde]
cash_int$ed<-c(cash_int$DATETIME[-1],eed+1)

ci<-data.table(indcode='s',date=as.Date('1900-01-01'),close=1,preclose=1,rt=1)[-1,]


for (ii in 1 : length(indcode_cash))
{
	ci<-rbind(ci,data.table(indcode=rep(indcode_cash[ii],times=length(wd$date)),date=wd$date,close=rep(0,times=length(wd$date)),preclose=rep(0,times=length(wd$date)),rt=rep(0,times=length(wd$date))))

}



for (ii in 1 : length(cash_int$DATETIME))
{
for (jj in 1 : length(icode))
{
  ci[date>=cash_int$DATETIME[ii] & date<cash_int$ed[ii] & indcode==indcode_cash[jj]]$close<-cash_int[ii,which(names(cash_int)==icode[jj]),with=FALSE]/100
}
}

ci365<-ci
ci365$indcode<-str_c(ci365$indcode,'365')

temp<-ci$date
month(temp)<-12
day(temp)<-31
ci$rt<-ci$close/yday(temp)
ci365$rt<-ci$close/365
ci<-rbind(ci,ci365)
bench_index<-rbind(mi,ci)
#合并基准利率类和市场指数类，作为基准计算的基础数据，并保存
dbWriteTable(con, "mindex",bench_index, overwrite=FALSE, append=TRUE,row.names=F)



#生成收益率的数据
tmp_rt<-fasset[,.(code,date,rt=unv*split/(preunv-div)-1,type='ad')]
tmp_rt_td<-fasset[,.(code,date,rt=unv*split/(preunv_td-div)-1,type='td')]
tmp_rt_ztd<-fasset[,.(code,date,rt=unv*split/(preunv_ztd-div)-1,type='ztd')]
tmp_rt_nav<-fasset[,.(code,date,rt=nav/snav-1,type='nav')]

#对于一些特殊的基金，直接给定特定日期的收益率
tmp_rt[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
tmp_rt[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
tmp_rt[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01

tmp_rt[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
tmp_rt[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
tmp_rt[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
tmp_rt[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
tmp_rt[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
tmp_rt[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
tmp_rt[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
tmp_rt[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
tmp_rt[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
tmp_rt[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
tmp_rt[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
tmp_rt[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
tmp_rt[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
tmp_rt[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)
  
tmp_rt_td[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
tmp_rt_td[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
tmp_rt_td[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01

tmp_rt_td[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
tmp_rt_td[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
tmp_rt_td[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
tmp_rt_td[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
tmp_rt_td[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
tmp_rt_td[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
tmp_rt_td[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
tmp_rt_td[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
tmp_rt_td[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
tmp_rt_td[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
tmp_rt_td[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
tmp_rt_td[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
tmp_rt_td[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
tmp_rt_td[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)
  
tmp_rt_ztd[code=='F166010' & date == as.Date('2014-06-17')]$rt<--0.003214470
tmp_rt_ztd[code=='155039' & date == as.Date('2014-06-17')]$rt<-0
tmp_rt_ztd[code=='155040' & date == as.Date('2014-06-17')]$rt<--0.01

tmp_rt_ztd[code=='166016' & date == as.Date('2013-07-30')]$rt<-0.000235121413995509
tmp_rt_ztd[code=='166016' & date == as.Date('2013-07-31')]$rt<--7.65857575039108E-05
tmp_rt_ztd[code=='166016' & date == as.Date('2014-01-30')]$rt<-0.00055181706813956
tmp_rt_ztd[code=='166016' & date == as.Date('2014-02-07')]$rt<-0.00248802866360442
tmp_rt_ztd[code=='166016' & date == as.Date('2014-07-30')]$rt<--0.000331599246851444
tmp_rt_ztd[code=='166016' & date == as.Date('2014-07-31')]$rt<-0.000162615555370449
tmp_rt_ztd[code=='166016' & date == as.Date('2015-01-30')]$rt<-0.000375049857613607
tmp_rt_ztd[code=='166016' & date == as.Date('2015-02-02')]$rt<-0.000944351822492528
tmp_rt_ztd[code=='166016' & date == as.Date('2015-07-30')]$rt<-0.000444195589399898
tmp_rt_ztd[code=='166016' & date == as.Date('2015-07-31')]$rt<-0.000554179764699958
tmp_rt_ztd[code=='166016' & date == as.Date('2016-01-29')]$rt<-0.00045106745531065
tmp_rt_ztd[code=='166016' & date == as.Date('2016-02-01')]$rt<--0.000145651883250331
tmp_rt_ztd[code=='166016' & date == as.Date('2016-02-02')]$rt<-0.00564923404226114
tmp_rt_ztd[code=='F166021' & date %in% as.Date(c('2014-5-27','2014-5-28','2014-5-29','2014-11-27','2014-11-28','2014-12-1','2015-5-27','2015-5-28','2015-5-29','2015-11-27','2015-11-30','2015-12-1','2016-5-27','2016-5-30','2016-12-1'))]$rt<-c(0.000526973760285099,0.000554261432409442,0.000763098390563322,0.000280849325514865,-1.8637270154942E-06,7.67581495579872E-05,-0.000256862288835813,-0.000100101673465858,-0.000434187295041077,0.000708130000000029,0.000611790000000001,0.00094751000000004,0.000303401600000086,0.000337068957248166,-0.00100200400801598)

#货币基金的收益率使用万份收益率/10000,对于定期报告的规则，货币的日期使用自然日而非交易日  
tmp_rt[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$rt<-fasset[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$treturn/10000
tmp_rt_td[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$rt<-fasset[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code]$treturn/10000
tmp_rt_ztd<-tmp_rt_ztd[!(code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code)]
tmp_rt_ztd<-rbind(tmp_rt_ztd,tmp_rt[code %in% fcode_dc[fcode_dc$innerclass=='货币型']$code])
tmp_rt_ztd$type<-'ztd'

#指定每个收益率的累计类型，分为sum 和 cum两种
tmp_rt$cumtype<-'cum'
tmp_rt_td$cumtype<-'cum'
tmp_rt_ztd$cumtype<-'cum'


#1、读取组合基准对应表
pbench<-data.table(read.xlsx2(paste(dbadr,'portbench.xlsx',sep=''),1,stringsAsFactors = FALSE))
pbench$sd<-as.Date(as.numeric(pbench$sd),origin='1899-12-30')
pbench$ed<-as.Date(as.numeric(pbench$ed),origin='1899-12-30')
  

#生成组合基准的收益率
#3-2合成基准的指数
benchr_temp<-merge(pbench,bench_index,by.x='indcode',by.y='indcode',all.x=TRUE,allow.cartesian=TRUE)
benchr_temp[is.na(rt)]$rt<-0
temp<-benchr_temp$date
month(temp)<-12
day(temp)<-31
benchr_temp$brtctr<-as.numeric(benchr_temp$wt)*benchr_temp$rt+as.numeric(benchr_temp$value)/yday(temp)
benchr<-benchr_temp[date>=sd & date<=ed,sum(brtctr),by=c('dbcode','name','method','date')]
names(benchr)<-c('code','name','method','date','rt')
benchr<-benchr[date>=std]

tmp<-ztd[date>=std]
tmp_dl<-data.table(date=wd$date,sd=wd$date,ed=wd$date)
dl<-tmp_dl[tmp,on=.(sd>preztd,ed<=date)]

fdl<-data.table(code='s',date=as.Date('2009-01-01'),sd=as.Date('2009-01-01'),ed=as.Date('2009-01-01'))[-1]
for (ii in 1 : length(fcode_dc$code))
{

if(fcode_dc[ii]$innerclass=='货币型')
{
  temp<-data.table(code=rep(fcode_dc$code[ii],times=length(wd[date>=fcode_dc$sopdate[ii]]$date)),date=wd[date>=fcode_dc$sopdate[ii]]$date,sd=wd[date>=fcode_dc$sopdate[ii]]$pred,ed=wd[date>=fcode_dc$sopdate[ii]]$date)
  
}
else
{
  temp<-data.table(code=rep(fcode_dc$code[ii],times=length(dl[date>=fcode_dc$sopdate[ii]]$date)),date=dl[date>=fcode_dc$sopdate[ii]]$date,sd=dl[date>=fcode_dc$sopdate[ii]]$sd,ed=dl[date>=fcode_dc$sopdate[ii]]$ed)
}
  fdl<-rbind(fdl,temp)
}


benchr<-merge(fdl,benchr,by.x=c('code','date'),by.y=c('code','date'),all=TRUE)
benchr<-benchr[!is.na(ed)]
benchr<-benchr[order(code,method,date,decreasing=FALSE)]
benchr[is.na(rt)]$rt<-0

imp<-unique(pbench[,c('code','name','method','sd','ed'),with=FALSE])
for (ii in 1 : length(imp$dbcode))
{
benchr[is.na(method) & date>=imp$sd[ii] & date<= imp$ed[ii] & code==imp$dbcode[ii]]$method<-imp$method[ii]
benchr[is.na(name) & date>=imp$sd[ii] & date<= imp$ed[ii] & code==imp$dbcode[ii]]$name<-imp$name[ii]
}


benchr_sum<-benchr[method=='sum',sum(rt),by=c('code','ed','name','method')]
benchr_cum<-benchr[method=='cum',prod(rt+1)-1,by=c('code','ed','name','method')]
benchr<-rbind(benchr_sum,benchr_cum)
names(benchr)<-c('code','date','name','method','rt')

#benchr$cumrt<-as.numeric(NA)
#benchr$cumrt_break<-as.numeric(NA)
benchr<-benchr[order(code,method,date,decreasing=FALSE)]

names(benchr)<-c('code','date','name','cumtype','rt')
benchr$type<-'bm'
benchr<-benchr[,.(code,date,rt,type,cumtype)]

fundrt<-rbind(tmp_rt,tmp_rt_td,tmp_rt_ztd,benchr)  
dbWriteTable(con, "fundrt",fundrt, overwrite=FALSE, append=TRUE,row.names=F)


#生成每日的中证800指数成分及权重信息
# sqlstr<-str_c('select S_INFO_WINDCODE as indcode, S_CON_WINDCODE as seccode, S_CON_INDATE as ssd, S_CON_OUTDATE as eed from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\'')
# indcons<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
# names(indcons)<-c('indcode','seccode','ssd','eed')
# indcons$ssd<-as.Date(indcons$ssd,format='%Y%m%d')
# indcons$eed<-as.Date(indcons$eed,format='%Y%m%d')
# indcons[is.na(eed)]$eed<-as.Date('2999-12-31')

# #读取股票权数数据
# sqlstr<-str_c('select S_INFO_WINDCODE,CHANGE_DT,S_INFO_WEIGHTS,S_INFO_INDEX_WEIGHTSRULE from NEWWIND.BOIndexWeightsWIND where  S_INFO_WINDCODE in (select UNIQUE(S_CON_WINDCODE) from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\' )')
# indcons_wn<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
# names(indcons_wn)<-c('seccode','chgd','wn','type')
# indcons_wn$chgd<-as.Date(indcons_wn$chgd,format='%Y%m%d')

# sqlstr<-str_c('select S_INFO_WINDCODE as seccode,TRADE_DT ,S_DQ_CLOSE as price from NEWWIND.AShareEODPrices where S_INFO_WINDCODE in (select UNIQUE(S_CON_WINDCODE) from NEWWIND.AIndexMembers where S_INFO_WINDCODE = \'000906.SH\' ) and TRADE_DT between \'', format(min(zotd$date),'%Y%m%d') ,'\' and \'',format(max(zotd$date),'%Y%m%d') ,'\'')
# secprice<-data.table(sqlQuery(winddb,sqlstr,as.is=TRUE))
# names(secprice)<-c('seccode','date','price')
# secprice$date<-as.Date(secprice$date,format='%Y%m%d')


# tmp_indcon_wt<-merge(indcons,indcons_wn,by='seccode',allow.cartesian=T,all.x=TRUE)

# ds<-unique(zotd[!is.na(pretd)]$date)
# for (ii in 1 : length(ds))
# {
#   tmp<-tmp_indcon_wt[ssd<=ds[ii] & eed>ds[ii] & type==7]
#   tmp<-merge(,secprice,by=c('seccode'))

# }




cons<-dbListConnections(MySQL())
for(con in cons) 
{
  dbDisconnect(con)
}
gc()
  



