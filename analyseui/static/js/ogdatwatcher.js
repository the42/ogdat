angular.module('ogdatanalysewebfrontend', ['ui.bootstrap', 'ngGrid']);
function CollapseDemoCtrl($scope) {
	$scope.isCollapsed = false;
}

function GridControll($scope, $http) {
	// TODO: set this hostname dynamically
	$http.get('http://localhost:5000/api/entities/').success(function(data) {
		$scope.myData = data;
	});
	$scope.gridOptions = { data: 'myData' };
}