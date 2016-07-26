var Meetup = require("meetup")
var elasticsearch = require('elasticsearch');
var config = require('./secrets.json');

var client = new elasticsearch.Client({
  host: config.esURL
});
var mup = new Meetup()

// TODO use timelion to create a moving average for yes/no
// TODO create a filter for 'yes' responses and do a metric on that, also for 'no' responses
mup.stream("/2/rsvps", function(stream){
  stream
    .on("data", function(item){
      var rsvp = {
        timestamp: new Date(item.mtime),
        rsvp_id: item.rsvp_id,
        response: item.response,
        guests: item.guests,
        member_name: item.member ? item.member.member_name : '',
        member_id: item.member ? item.member.member_id : '',
        event_name: item.event ? item.event.event_name : '',
        event_id: item.event ? item.event.event_id : '',
        event_time: item.event ? item.event.time : '',
        group_city: item.group ? item.group.group_city : '',
        group_country: item.group ? item.group.group_country : '',
        group_name: item.group ? item.group.group_name : '',
        group_id: item.group ? item.group.group_id : '',
        group_location: item.group && item.group.group_lat ? item.group.group_lat + "," + item.group.group_lon : '',
        group_topics: item.group ? item.group.group_topics : '',
        venue_name: item.venue ? item.venue.venue_name : '',
        venue_id: item.venue ? item.venue.venue_id : '',
        venue_location: item.venue && item.venue.venue_lat ? item.venue.venue_lat + "," + item.venue.venue_lon : ''
      }
      postRSVPToES(rsvp);
    }).on("error", function(e) {
      console.log("error! " + e);
    });
});

function postRSVPToES(rsvp) {
  client.create({
    index: 'meetup_rsvp_2',
    type: 'rsvps',
    body: rsvp
  }, function (error) {
    if (error) {
      console.trace(error);
    } else {
      console.log('RSVP added successfully');
    }
  });
}
