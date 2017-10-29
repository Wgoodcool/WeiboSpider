# WeiboSpider
这是一个新浪微博爬虫，刚刚起步，后续会逐渐完善。整体的架构图过阵子补上

# TODO:
* 增加日志记录功能
* 将downloader改为协程，加快处理速度
* 储存解析好的DNS结果，供downloader直接使用，减少DNS请求时间
* 加上断点续抓功能

## 微博架构图

#### database
![database](http://img.blog.csdn.net/20171027131735648?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdTAxMTY3NTc0NQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

#### Schedule
![Schedule](http://img.blog.csdn.net/20171027132055509?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdTAxMTY3NTc0NQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

#### Downloader
![Downloader](http://img.blog.csdn.net/20171027132143442?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdTAxMTY3NTc0NQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

#### Error
![Error](http://img.blog.csdn.net/20171027132219073?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdTAxMTY3NTc0NQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

#### Spider
![Spider](http://img.blog.csdn.net/20171027132249955?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvdTAxMTY3NTc0NQ==/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)

