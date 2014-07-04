<!DOCTYPE html>
<html>
  <head>
    <title>
      以图搜图
    </title>
    <style type="text/css">
      h1 {font-size:20px; text-align:center;}
      h2 {font-size:20px;}
      .btns{width:120px; height:80px;font-size:60px;}
    </style>
</head>
  <body>
    <h1> 请输入图片的url:</h1>
    <form method="post" action="/result" align="center">
      <p><input type="text" name="query_imgurl" style="width:600px;height:25px"></p>
      <h2>选择输入图片的所属类别:</h2>
        <input type="radio" name="category" value="clothes"/> 衣服
        <input type="radio" name="category" value="shoe"/> 鞋子
        <input type="radio" name="category" value="bag"/> 包包
	<input type="radio" name="category" value="acc"/> 配饰
      <br />
      <br />
      <input type="submit" class= "btns"value="搜索">
    </form>
  </body>
</html>

