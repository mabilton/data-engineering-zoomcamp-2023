"use strict";(self.webpackChunk_infuseai_piperider_ui=self.webpackChunk_infuseai_piperider_ui||[]).push([[173],{6173:function(e,n,t){t.r(n),t.d(n,{default:function(){return w}});var i=t(1431),r=t(7615),a=t(2753),l=t(1756),s=t(2847),o=t(2471),d=t(7708),c=t(7437),u=t(4641),m=t(962),x=t(3512),p=t(3214),h=t(2282),b=t(8685),j=t(421),f=t(3015),g=t(3851),v=t(5887);function w(e){var n=e.data,t=e.columnName,w=e.tableName;(0,b.jr)("Single Report: Table Column Details"),(0,b.Lp)({eventName:b.mk.PAGE_VIEW,eventProperties:{type:b.ll,page:"column-details-page"}});var y=(0,l.useState)(0),S=(0,i.Z)(y,2),C=S[0],D=S[1];(0,f.n)((function(e){return e.setReportRawData}))({base:n});var R=f.n.getState(),P=R.tableColumnsOnly,_=void 0===P?[]:P,T=R.rawData,k=_.find((function(e){return(0,i.Z)(e,1)[0]===w})),E=0===t.length,N=n.tables[w],O=N.columns[t],A=O||{},I=A.type,M=A.histogram,Q=A.schema_type,W=(0,p.MM)(O),F=W.backgroundColor,G=W.icon;if(!w||!N||!k)return(0,v.jsx)(s.o,{isSingleReport:!0,children:(0,v.jsx)(x.J,{text:"No profile data found for table name: ".concat(w)})});var K=(0,p.hG)(I);return(0,v.jsx)(s.o,{isSingleReport:!0,children:(0,v.jsx)(g.$,{initAsExpandedTables:!0,rawData:T,tableColEntries:_,tableName:w,columnName:t,singleOnly:!0,children:E?(0,v.jsxs)(r.P4,{h:c.t4,overflowY:"auto",p:10,children:[(0,v.jsx)(j.Q,{title:N.name,subtitle:"Table",infoTip:N.description,mb:5}),(0,v.jsxs)(a.mQ,{mt:3,defaultIndex:0,children:[(0,v.jsxs)(a.td,{children:[(0,v.jsx)(a.OK,{children:"Overview"}),(0,v.jsx)(a.OK,{children:"Schema"})]}),(0,v.jsxs)(a.nP,{children:[(0,v.jsx)(a.x4,{children:(0,v.jsx)(r.rj,{templateColumns:"1fr 1fr",gap:3,children:(0,v.jsx)(h.m,{tableDatum:N})})}),(0,v.jsx)(a.x4,{children:(0,v.jsx)(b.rH,{baseTableEntryDatum:null===k||void 0===k?void 0:k[1].base,singleOnly:!0})})]})]})]}):(0,v.jsxs)(r.rj,{templateColumns:"1fr 1fr",templateRows:"8em 1fr 1fr ".concat(K?"1fr":""),gridAutoFlow:"column",width:"100%",pb:5,children:[(0,v.jsx)(r.P4,{colSpan:2,p:9,children:(0,v.jsx)(j.Q,{title:t,subtitle:Q,infoTip:null===O||void 0===O?void 0:O.description,icon:G,iconColor:F,mb:5})}),(0,v.jsx)(r.P4,{colSpan:1,px:10,bg:"gray.50",borderRight:c.dx,children:(0,v.jsx)(o.t,{columnDatum:O,hasAnimation:!0})}),(0,v.jsx)(r.P4,{gridRow:"auto",px:10,bg:"gray.50",borderRight:c.dx,children:(0,p.jl)(I)&&(0,v.jsx)(v.Fragment,{children:(0,v.jsx)(m.E,{columnDatum:O})})}),K&&M&&(0,v.jsx)(r.P4,{bg:"gray.50",minWidth:"1px",borderRight:c.dx,p:10,children:(0,v.jsx)(u.M,{columnDatum:O})}),(0,v.jsx)(r.P4,{colSpan:1,rowSpan:K?3:2,minWidth:0,px:10,bg:"gray.50",children:(0,v.jsx)(d.r,{baseColumnDatum:O,hasAnimation:!0,tabIndex:C,onSelectTab:function(e){return D(e)}})})]})})})}}}]);
//# sourceMappingURL=173.405a9bd7.chunk.js.map