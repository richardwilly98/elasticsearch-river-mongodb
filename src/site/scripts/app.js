'use strict';

var mongoDBRiverApp = angular.module('mongoDBRiverApp', ['ngResource', 'ui.bootstrap']);

mongoDBRiverApp.constant('appSettings', {
  defaultRefresh: 5000
});

mongoDBRiverApp.controller('MainCtrl', function ($log, $scope, $resource, $timeout, appSettings) {
  var riverResource = $resource('/_river/:type/:river/:action' , {type:'@type', river:'@river', page:'@page'},
    {
      list: {method:'GET', params: {action: 'list'}},
      start: {method:'POST', params: {action: 'start'}},
      stop: {method:'POST', params: {action: 'stop'}},
      delete: {method:'POST', params: {action: 'delete'}}
    }
  );
  var timeoutId;

  $scope.uiSettings = {};

  $scope.getUISettings = function(riverName) {
    if (typeof $scope.uiSettings[riverName] == 'undefined') {
      $scope.uiSettings[riverName] = {};
    }

    return $scope.uiSettings[riverName];
  }

  $scope.rivers = [];
  $scope.type = null;
  $scope.pages = 0;
  $scope.page = 0;
  $scope.refresh = { label: 'Auto-refresh disabled', enabled: false };
  $scope.next = { label: 'Next Page', enabled: false }
  $scope.prev = { label: 'Previous Page', enabled: false }

  $scope.nextPage = function() {
    if($scope.next.enabled) {
      $scope.list(null, $scope.page+1);
    }
  }

  $scope.prevPage = function() {
    if($scope.prev.enabled) {
      $scope.list(null, $scope.page-1);
    }
  }

  function autoRefresh() {
    timeoutId = $timeout(function() {
      $scope.list();
      autoRefresh();
    }, appSettings.defaultRefresh);
  }

  $scope.updateTimer = function(enabled) {
    $log.log('updateTimer - ' + enabled)
    if (!enabled) {
      autoRefresh();
      $scope.refresh.label = 'Auto-refresh enabled';
      $scope.refresh.enabled = true;
    } else {
      $timeout.cancel(timeoutId);
      timeoutId = null;
      $scope.refresh.label = 'Auto-refresh disabled';
      $scope.refresh.enabled = false;
    }
  }

  $scope.list = function(type, page){
    $log.log('list river type: ' + type);
    $scope.type = type || 'mongodb';
    var data = {'type': $scope.type};
    if(page != undefined && page != null) {
      data.page = page;
    }
    var rivers = riverResource.list(data, function() {
      $log.log('rivers count: ' + rivers.hits);
      $scope.rivers = rivers.results;
      $scope.pages = rivers.pages;
      $scope.page = rivers.page;
      if($scope.page > 1) {
        $scope.prev.enabled = true;
      } else {
        $scope.prev.enabled = false;
      }
      if($scope.page < $scope.pages) {
        $scope.next.enabled = true;
      } else {
        $scope.next.enabled = false;
      }
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

  $scope.delete = function(name){
    $log.log('delete: ' + name);
    riverResource.delete({'type': $scope.type, 'river': name}, function() {
      $scope.list();
    });
  };

  $scope.toString = function(object){
    var value = JSON.stringify(angular.copy(object), undefined, 2);
    return value;
  };

  $scope.list();
});
