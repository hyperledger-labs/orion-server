"use strict";(self.webpackChunkdocs=self.webpackChunkdocs||[]).push([[4270],{3905:(e,t,n)=>{n.d(t,{Zo:()=>d,kt:()=>g});var r=n(7294);function a(e,t,n){return t in e?Object.defineProperty(e,t,{value:n,enumerable:!0,configurable:!0,writable:!0}):e[t]=n,e}function o(e,t){var n=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);t&&(r=r.filter((function(t){return Object.getOwnPropertyDescriptor(e,t).enumerable}))),n.push.apply(n,r)}return n}function i(e){for(var t=1;t<arguments.length;t++){var n=null!=arguments[t]?arguments[t]:{};t%2?o(Object(n),!0).forEach((function(t){a(e,t,n[t])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(n)):o(Object(n)).forEach((function(t){Object.defineProperty(e,t,Object.getOwnPropertyDescriptor(n,t))}))}return e}function l(e,t){if(null==e)return{};var n,r,a=function(e,t){if(null==e)return{};var n,r,a={},o=Object.keys(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||(a[n]=e[n]);return a}(e,t);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)n=o[r],t.indexOf(n)>=0||Object.prototype.propertyIsEnumerable.call(e,n)&&(a[n]=e[n])}return a}var s=r.createContext({}),c=function(e){var t=r.useContext(s),n=t;return e&&(n="function"==typeof e?e(t):i(i({},t),e)),n},d=function(e){var t=c(e.components);return r.createElement(s.Provider,{value:t},e.children)},u="mdxType",p={inlineCode:"code",wrapper:function(e){var t=e.children;return r.createElement(r.Fragment,{},t)}},m=r.forwardRef((function(e,t){var n=e.components,a=e.mdxType,o=e.originalType,s=e.parentName,d=l(e,["components","mdxType","originalType","parentName"]),u=c(n),m=a,g=u["".concat(s,".").concat(m)]||u[m]||p[m]||o;return n?r.createElement(g,i(i({ref:t},d),{},{components:n})):r.createElement(g,i({ref:t},d))}));function g(e,t){var n=arguments,a=t&&t.mdxType;if("string"==typeof e||a){var o=n.length,i=new Array(o);i[0]=m;var l={};for(var s in t)hasOwnProperty.call(t,s)&&(l[s]=t[s]);l.originalType=e,l[u]="string"==typeof e?e:a,i[1]=l;for(var c=2;c<o;c++)i[c]=n[c];return r.createElement.apply(null,i)}return r.createElement.apply(null,n)}m.displayName="MDXCreateElement"},4285:(e,t,n)=>{n.r(t),n.d(t,{contentTitle:()=>i,default:()=>d,frontMatter:()=>o,metadata:()=>l,toc:()=>s});var r=n(7462),a=(n(7294),n(3905));const o={id:"binary",title:"Orion Executable"},i=void 0,l={unversionedId:"getting-started/launching-one-node/binary",id:"getting-started/launching-one-node/binary",isDocsHomePage:!1,title:"Orion Executable",description:"\x3c!--",source:"@site/docs/getting-started/launching-one-node/binary.md",sourceDirName:"getting-started/launching-one-node",slug:"/getting-started/launching-one-node/binary",permalink:"/orion-server/docs/getting-started/launching-one-node/binary",tags:[],version:"current",frontMatter:{id:"binary",title:"Orion Executable"},sidebar:"Documentation",previous:{title:"Overview",permalink:"/orion-server/docs/getting-started/launching-one-node/overview"},next:{title:"Orion Docker",permalink:"/orion-server/docs/getting-started/launching-one-node/docker"}},s=[{value:"1) Prerequisites",id:"1-prerequisites",children:[],level:2},{value:"2) Build",id:"2-build",children:[],level:2},{value:"3) Start",id:"3-start",children:[],level:2}],c={toc:s};function d(e){let{components:t,...n}=e;return(0,a.kt)("wrapper",(0,r.Z)({},c,n,{components:t,mdxType:"MDXLayout"}),(0,a.kt)("h2",{id:"1-prerequisites"},"1) Prerequisites"),(0,a.kt)("p",null,"To build an executable binary file, the following prerequisites should be installed on the platform on which Orion will be deployed."),(0,a.kt)("ul",null,(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("a",{parentName:"strong",href:"https://golang.org/"},"Go Programming Language")),": The database uses the Go Programming Language for many of its components. Go version 1.16.x or higher is required."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("a",{parentName:"strong",href:"https://man7.org/linux/man-pages/man1/make.1.html"},"Make")),": To build a binary file and execute a unit-test, the ",(0,a.kt)("inlineCode",{parentName:"li"},"make")," utility is required."),(0,a.kt)("li",{parentName:"ul"},(0,a.kt)("strong",{parentName:"li"},(0,a.kt)("a",{parentName:"strong",href:"https://github.com/git-guides/install-git"},"Git")),": Git is used to clone the code repository.")),(0,a.kt)("h2",{id:"2-build"},"2) Build"),(0,a.kt)("p",null,"To build the executable binary, the following steps must be executed:"),(0,a.kt)("ol",null,(0,a.kt)("li",{parentName:"ol"},"Create the required directory using the command ",(0,a.kt)("inlineCode",{parentName:"li"},"mkdir -p github.com/hyperledger-labs"),"."),(0,a.kt)("li",{parentName:"ol"},"Change the current working directory to the above created directory by issuing the command ",(0,a.kt)("inlineCode",{parentName:"li"},"cd github.com/hyperledger-labs"),"."),(0,a.kt)("li",{parentName:"ol"},"Clone the server repository with ",(0,a.kt)("inlineCode",{parentName:"li"},"git clone https://github.com/hyperledger-labs/orion-server"),"."),(0,a.kt)("li",{parentName:"ol"},"Change the current working directory to the repository root directory by issuing ",(0,a.kt)("inlineCode",{parentName:"li"},"cd orion-server"),"."),(0,a.kt)("li",{parentName:"ol"},"To build the binary executable file, issue the command ",(0,a.kt)("inlineCode",{parentName:"li"},"make binary"),", which creates a ",(0,a.kt)("inlineCode",{parentName:"li"},"bin")," folder in the current directory. The ",(0,a.kt)("inlineCode",{parentName:"li"},"bin")," holds four executable\nfiles named ",(0,a.kt)("inlineCode",{parentName:"li"},"bdb"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"signer"),", ",(0,a.kt)("inlineCode",{parentName:"li"},"encoder"),", and ",(0,a.kt)("inlineCode",{parentName:"li"},"decoder"),"."),(0,a.kt)("li",{parentName:"ol"},"Run the ",(0,a.kt)("inlineCode",{parentName:"li"},"bdb")," executable by issuing the following command:")),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"./bin/bdb\n")),(0,a.kt)("p",null,"If the above command execution results in the following output, the build was successful."),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-text"},'To start and interact with a blockchain database server.\nUsage:\n  bdb [command]\nAvailable Commands:\n  help        Help about any command\n  start       Starts a blockchain database\n  version     Print the version of the blockchain database server.\nFlags:\n  -h, --help   help for bdb\nUse "bdb [command] --help" for more information about a command.\n')),(0,a.kt)("p",null,"For additional health checks, we can run ",(0,a.kt)("inlineCode",{parentName:"p"},"make test")," to ensure all tests pass."),(0,a.kt)("h2",{id:"3-start"},"3) Start"),(0,a.kt)("p",null,"To start a node, we need a certificate authority and crypto materials for the node and admin users. To simplify this task, we have provided sample\ncrypto materials and configuration files in the ",(0,a.kt)("inlineCode",{parentName:"p"},"deployment/sample/")," folder. In order to understand the configuration files and procedure for creating crypto\nmaterials, refer to ",(0,a.kt)("a",{parentName:"p",href:"crypto-materials"},"crypto materials"),"."),(0,a.kt)("p",null,"Let's start a node with the sample configuration:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-shell"},"./bin/bdb start --configpath ./deployment/config-local/config.yml\n")),(0,a.kt)("p",null,"The output of the above command would be something similar to the following:"),(0,a.kt)("pre",null,(0,a.kt)("code",{parentName:"pre",className:"language-text"},"2021/06/16 18:06:34 Starting a blockchain database\n2021-06-16T18:06:35.151+0530    INFO    bdb-node-1  blockprocessor/processor.go:263 Registering listener [transactionProcessor]\n2021-06-16T18:06:35.151+0530    INFO    bdb-node-1  txreorderer/txreorderer.go:59   starting the transactions reorderer\n2021-06-16T18:06:35.151+0530    INFO    bdb-node-1  blockcreator/blockcreator.go:74 starting the block creator\n2021-06-16T18:06:35.151+0530    INFO    bdb-node-1  replication/blockreplicator.go:73   starting the block replicator\n2021-06-16T18:06:35.153+0530    INFO    bdb-node-1  server/server.go:113    Starting to serve requests on: 127.0.0.1:6001\n")),(0,a.kt)("p",null,"Congratulations! We have started a node successfully."))}d.isMDXComponent=!0}}]);