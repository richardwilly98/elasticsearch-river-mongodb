'use strict';

var mongoDBRiverApp = angular.module('mongoDBRiverApp', ['ngResource', 'ui.bootstrap']);

mongoDBRiverApp.controller('MainCtrl', function ($log, $scope, $resource) {
  var riverResource = $resource('/_river/:type/:river/:action' , {type:'@type', river:'@river'}, {
	  list: {method:'GET', params: {action: '_list'}, isArray: true},
      start: {method:'POST', params: {action: '_start'}},
      stop: {method:'POST', params: {action: '_stop'}}
    });

  $scope.rivers = [];
  $scope.type = null;
  $scope.list = function(type){
    $log.log('list river type: ' + type);
    $scope.type = type;
    var rivers = riverResource.list({'type': type}, function() {
      $log.log('rivers count: ' + rivers.length);
      $scope.rivers = rivers;
    });
  };
  $scope.start = function(name){
    $log.log('start: ' + name);
    riverResource.start({'type': $scope.type, 'river': name}, function(river, response) {
      setRiverEnabled(name, true);
      // var river = _.find($scope.rivers, {'_name': name});
      // if (river !== undefined) {
      //   river._enabled = true;
      // }
    });
  };
  $scope.stop = function(name){
    $log.log('stop: ' + name);
    riverResource.stop({'type': $scope.type, 'river': name}, function() {
            setRiverEnabled(name, false);
    });
  };
  $scope.toString = function(object){
    var value = JSON.stringify(angular.copy(object), undefined, 2);
    return value;
  };
  function setRiverEnabled(name, enabled) {
      var river = _.find($scope.rivers, {'_name': name});
      if (river !== undefined) {
        river._enabled = enabled;
      }
  }
});
