"use strict";(self.webpackChunkdocs=self.webpackChunkdocs||[]).push([[3085],{4766:(e,a,t)=>{t.r(a),t.d(a,{default:()=>d});var l=t(7294),n=t(6010),s=t(9148),c=t(3905),i=t(2550),m=t(3931),o=t(3773);const r="mdxPageWrapper_C+hS";const d=function(e){const{content:a}=e,{frontMatter:t,metadata:d}=a,{title:v,description:g,wrapperClassName:N,hide_table_of_contents:f}=t,{permalink:k}=d;return l.createElement(s.Z,{title:v,description:g,permalink:k,wrapperClassName:N??o.kM.wrapper.mdxPages,pageClassName:o.kM.page.mdxPage},l.createElement("main",{className:"container container--fluid margin-vert--lg"},l.createElement("div",{className:(0,n.Z)("row",r)},l.createElement("div",{className:(0,n.Z)("col",!f&&"col--8")},l.createElement(c.Zo,{components:i.Z},l.createElement(a,null))),!f&&a.toc&&l.createElement("div",{className:"col col--2"},l.createElement(m.Z,{toc:a.toc,minHeadingLevel:t.toc_min_heading_level,maxHeadingLevel:t.toc_max_heading_level})))))}},3931:(e,a,t)=>{t.d(a,{Z:()=>m});var l=t(7462),n=t(7294),s=t(6010),c=t(2012);const i="tableOfContents_1gSR";const m=function(e){let{className:a,...t}=e;return n.createElement("div",{className:(0,s.Z)(i,"thin-scrollbar",a)},n.createElement(c.Z,(0,l.Z)({},t,{linkClassName:"table-of-contents__link toc-highlight",linkActiveClassName:"table-of-contents__link--active"})))}},2012:(e,a,t)=>{t.d(a,{Z:()=>i});var l=t(7462),n=t(7294),s=t(3773);function c(e){let{toc:a,className:t,linkClassName:l,isChild:s}=e;return a.length?n.createElement("ul",{className:s?void 0:t},a.map((e=>n.createElement("li",{key:e.id},n.createElement("a",{href:`#${e.id}`,className:l??void 0,dangerouslySetInnerHTML:{__html:e.value}}),n.createElement(c,{isChild:!0,toc:e.children,className:t,linkClassName:l}))))):null}function i(e){let{toc:a,className:t="table-of-contents table-of-contents__left-border",linkClassName:i="table-of-contents__link",linkActiveClassName:m,minHeadingLevel:o,maxHeadingLevel:r,...d}=e;const v=(0,s.LU)(),g=o??v.tableOfContents.minHeadingLevel,N=r??v.tableOfContents.maxHeadingLevel,f=(0,s.DA)({toc:a,minHeadingLevel:g,maxHeadingLevel:N}),k=(0,n.useMemo)((()=>{if(i&&m)return{linkClassName:i,linkActiveClassName:m,minHeadingLevel:g,maxHeadingLevel:N}}),[i,m,g,N]);return(0,s.Si)(k),n.createElement(c,(0,l.Z)({toc:f,className:t,linkClassName:i},d))}}}]);