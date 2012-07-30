(defproject storm/storm-mesos "0.0.1-SNAPSHOT"
  :description "Storm integration with the Mesos cluster manager"
  :source-path "src/clj"
  :java-source-path "src/jvm"
  :javac-options {:debug "true" :fork "true"}
  :repositories {"releases" "http://artifactory.local.twitter.com/libs-releases-local"
                 "snapshots" "http://artifactory.local.twitter.com/libs-snapshots-local"}
  :dependencies [
[org.apache.mesos/mesos "0.9.0-120"] ;; TODO: needs to be made public
]
  :dev-dependencies [
[storm "0.8.0-SNAPSHOT"]
[org.clojure/clojure "1.4.0"]
])
