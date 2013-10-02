'use strict';

var mongoDBRiverApp = angular.module('mongoDBRiverApp', ['ngResource', 'ui.bootstrap']);

mongoDBRiverApp.controller('MainCtrl', function ($log, $scope, $resource) {
  var riverResource = $resource('/_river/:type/:river/:action' , {type:'@type', river:'@river'},
    {
	  list: {method:'GET', params: {action: 'list'}, isArray: true},
      start: {method:'POST', params: {action: 'start'}},
      stop: {method:'POST', params: {action: 'stop'}}
    }
  );

  $scope.rivers = [];
  $scope.type = null;

  $scope.list = function(type){
    $log.log('list river type: ' + type);
    $scope.type = type || 'mongodb';
    var rivers = riverResource.list({'type': $scope.type}, function() {
      $log.log('rivers count: ' + rivers.length);
      $scope.rivers = rivers;
    });
  };

  $scope.start = function(name){
    $log.log('start: ' + name);
    riverResource.start({'type': $scope.type, 'river': name}, function(river, response) {
      $scope.list();
    });
  };

  $scope.stop = function(name){
    $log.log('stop: ' + name);
    riverResource.stop({'type': $scope.type, 'river': name}, function() {
      $scope.list();
    });
  };

  $scope.toString = function(object){
    var value = JSON.stringify(angular.copy(object), undefined, 2);
    return value;
  };

  $scope.list();
});
