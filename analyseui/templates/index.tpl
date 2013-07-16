{{{define "index"}}}<!DOCTYPE html>
<html ng-app="ogdatanalysewebfrontend">
	<head>
		<meta charset="UTF-8"/>
		<title>A title</title>
		<link href="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.2/css/bootstrap-combined.min.css" rel="stylesheet"/>
		<link href="./static/css/ng-grid.min.css" rel="stylesheet"/>
		<link href="./static/css/custom-grid.css" rel="stylesheet"/>
	</head>
	<body>
		<div>
			<label>Name:</label>
			<input type="text" ng-model="yourName" placeholder="Enter a name here"/>
			<hr/>
			<h1>Hello {{yourName}}!</h1>
		</div>
		<div ng-controller="CollapseDemoCtrl">
			<button class="btn" ng-click="isCollapsed = !isCollapsed">Toggle collapse</button>
			<hr/>
			<div collapse="isCollapsed">
				<div class="well well-large">Some content</div>
				<div ng-controller="GridControll">
					<div class="gridStyle" ng-grid="gridOptions">
					</div>
				</div>				
			</div>
		</div>
		<script src="//ajax.googleapis.com/ajax/libs/jquery/2.0.3/jquery.min.js">
		</script>
		<script src="https://ajax.googleapis.com/ajax/libs/angularjs/1.1.5/angular.min.js">
		</script>
		<script src="http://angular-ui.github.io/bootstrap/ui-bootstrap-tpls-0.4.0.js">
		</script>
		<script src="./static/js/ng-grid-2.0.7.min.js">
                </script>
		<script src="./static/js/ogdatwatcher.js"></script>
	</body>
</html>{{{end}}}