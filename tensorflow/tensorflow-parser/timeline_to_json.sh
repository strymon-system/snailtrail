for ex in timelines/* ; do
    for json in $ex/timelines/* ; do
        mkdir -p msgpacks/$(basename $ex)
        msgpack="msgpacks/$(basename $ex)/$(basename $json .json).msgpack"
        output="msgpacks/$(basename $ex)/$(basename $json .json).txt"
        if [ ! -e $msgpack ] ; then
            cargo run -- -- $json -o $msgpack > $output
        fi
    done
done
