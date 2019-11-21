module qianxunke-fastdfs

go 1.13

require (
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/astaxie/beego v1.12.0
	github.com/aws/aws-sdk-go v1.25.31 // indirect
	github.com/bmizerany/pat v0.0.0-20170815010413-6226ea591a40 // indirect
	github.com/cihub/seelog v0.0.0-20170130134532-f561c5e57575 // indirect
	github.com/deckarep/golang-set v1.7.1
	github.com/eventials/go-tus v0.0.0-20190617130015-9db47421f6a0
	github.com/gin-gonic/gin v1.4.0 // indirect
	github.com/go-ole/go-ole v1.2.4 // indirect
	github.com/golang/mock v1.3.1 // indirect
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/jinzhu/gorm v1.9.11
	github.com/json-iterator/go v1.1.8
	github.com/kr/pretty v0.1.0 // indirect
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646
	github.com/radovskyb/watcher v1.0.7
	github.com/shirou/gopsutil v2.19.10+incompatible
	github.com/sjqzhang/googleAuthenticator v0.0.0-20160926062737-f198f070e0b1 // indirect
	github.com/sjqzhang/goutil v0.0.0-20190521040736-8e3b861db0d2
	github.com/sjqzhang/seelog v0.0.0-20180104061743-556439109558
	github.com/sjqzhang/tusd v0.0.0-20190220031306-a6a9d78ef54a
	github.com/syndtr/goleveldb v1.0.0
	google.golang.org/appengine v0.0.0-00010101000000-000000000000 // indirect
	gopkg.in/Acconut/lockfile.v1 v1.1.0 // indirect
	gopkg.in/yaml.v2 v2.2.2

)

replace (
	cloud.google.com/go => github.com/GoogleCloudPlatform/google-cloud-go v0.34.0
	golang.org/x/crypto => github.com/golang/crypto v0.0.0-20190313024323-a1f597ede03a
	golang.org/x/exp => github.com/golang/exp v0.0.0-20190510132918-efd6b22b2522
	golang.org/x/lint => github.com/golang/lint v0.0.0-20190409202823-959b441ac422
	golang.org/x/net => github.com/golang/net v0.0.0-20190318221613-d196dffd7c2b
	golang.org/x/oauth2 => github.com/golang/oauth2 v0.0.0-20190523182746-aaccbc9213b0
	golang.org/x/sync => github.com/golang/sync v0.0.0-20190227155943-e225da77a7e6
	golang.org/x/sys => github.com/golang/sys v0.0.0-20190318195719-6c81ef8f67ca
	golang.org/x/text => github.com/golang/text v0.3.0
	golang.org/x/time => github.com/golang/time v0.0.0-20190308202827-9d24e82272b4
	golang.org/x/tools => github.com/golang/tools v0.0.0-20190529010454-aa71c3f32488
	google.golang.org/appengine => github.com/golang/appengine v1.6.1-0.20190515044707-311d3c5cf937
	google.golang.org/genproto => github.com/google/go-genproto v0.0.0-20190522204451-c2c4e71fbf69
	google.golang.org/grpc => github.com/grpc/grpc-go v1.21.0
)
