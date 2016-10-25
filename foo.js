var migration = {
    _id : "CS-12345",
    runBefore : [ "CS-12332", "CS-12221" ],
    onLoad : function(state) {
        print("Whoo hoo this is onLoad from foo");
        printjson(state);
    },
    up : function() {
        print("Hello what's up()!!")
    },
    down : function() {
        print("Say are you feeling down()???");
    },
    info : function() {
        print("Hello, I'm the info for this migration");
    }
}
// This will be the convention that each migration 
// needs to follow
mongrate.exports = migration;
