package main

import (
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/gorilla/mux"
)

const SocketFile = "/var/run/blocker.sock"

func main() {
	log("blocker: starting up...\n")

	d, err := NewEbsVolumeDriver()
	if err != nil {
		logError("Failed to create an EBS driver: %s.\n", err)
		return
	}

	// Manufacture a socket for communication with Docker.
	l, err := net.Listen("unix", SocketFile)
	if err != nil {
		logError("Failed to listen on socket %s: %s.\n", SocketFile, err)
		return
	}
	defer l.Close()

	// Make a channel that signals program exit.
	exit := make(chan bool, 1)

	// Listen to important OS signals, so we trigger exit cleanly.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, os.Kill, syscall.SIGTERM)
	go func() {
		sig := <-signals
		log("Caught signal %s: shutting down.\n", sig)
		// TODO: forcibly unmount all volumes.
		exit <- true
	}()

	// Now listen for HTTP calls from Docker.
	handler := makeRoutes(d)
	go func() {
		log("Ready to go; listening on socket %s...\n", SocketFile)
		err = http.Serve(l, handler)
		if err != nil {
			logError("HTTP server error: %s.\n", err)
		}
		exit <- true
	}()

	// Block until the program exits.
	<-exit
}

func makeRoutes(d VolumeDriver) http.Handler {
	r := mux.NewRouter()
	// TODO: permit options in the name string.
	r.HandleFunc("/Plugin.Activate", servePluginActivate)
	r.HandleFunc("/VolumeDriver.Create", serveVolumeCreate(d.Create))
	r.HandleFunc("/VolumeDriver.Mount", serveVolumeComplex(d.Mount))
	r.HandleFunc("/VolumeDriver.Path", serveVolumeComplex(d.Path))
	r.HandleFunc("/VolumeDriver.Remove", serveVolumeSimple(d.Remove))
	r.HandleFunc("/VolumeDriver.Unmount", serveVolumeSimple(d.Unmount))

	r.HandleFunc("/VolumeDriver.Get", serveVolumeInstance(d.Get))
	r.HandleFunc("/VolumeDriver.List", serveVolumeList(d.List))
	r.HandleFunc("/VolumeDriver.Capabilities", serveDriverCapabilities(d.Capabilities))
	return r
}

type pluginInfoResponse struct {
	Implements []string
}

func servePluginActivate(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(pluginInfoResponse{
		Implements: []string{"VolumeDriver"},
	})
}

type volumeRequest struct {
	Name string
}

type volumeSimpleResponse struct {
	Err string
}

func serveVolumeSimple(f func(string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var vol volumeRequest
		err := json.NewDecoder(r.Body).Decode(&vol)
		if err == nil {
			err = f(vol.Name)
			log("\tdone: (%s): %v\n", vol.Name, err)
		}
		var errs string
		if err != nil {
			errs = err.Error()
		}
		json.NewEncoder(w).Encode(volumeSimpleResponse{
			Err: errs,
		})
	}
}

type volumeCreateRequest struct {
	Name string
	Opts map[string]string
}

func serveVolumeCreate(f func(string, map[string]string) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var vol volumeCreateRequest
		err := json.NewDecoder(r.Body).Decode(&vol)
		if err == nil {
			err = f(vol.Name, vol.Opts)
			log("\tdone: (%s): %v\n", vol.Name, err)
		}
		var errs string
		if err != nil {
			errs = err.Error()
		}
		json.NewEncoder(w).Encode(volumeSimpleResponse{
			Err: errs,
		})
	}
}

type volumeComplexResponse struct {
	Mountpoint string
	Err        string
}

func serveVolumeComplex(f func(string) (string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var vol volumeRequest
		err := json.NewDecoder(r.Body).Decode(&vol)
		var mountpoint string
		if err == nil {
			mountpoint, err = f(vol.Name)
			log("\tdone: (%s): (%s, %v)\n", vol.Name, mountpoint, err)
		}
		var errs string
		if err != nil {
			errs = err.Error()
		}
		json.NewEncoder(w).Encode(volumeComplexResponse{
			Mountpoint: mountpoint,
			Err:        errs,
		})
	}
}

type volumeInstanceResponse struct {
	Volume     map[string]string
	Err        string
}

func serveVolumeInstance(f func(string) (map[string]string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var vol volumeRequest
		err := json.NewDecoder(r.Body).Decode(&vol)
		var volume_info = make(map[string]string)
		if err == nil {
			volume_info, err = f(vol.Name)
			log("\tdone: (%s): (%s, %v)\n", vol.Name, volume_info, err)
		}
		var errs string
		if err != nil {
			errs = err.Error()
		}
		json.NewEncoder(w).Encode(volumeInstanceResponse{
			Volume:     volume_info,
			Err:        errs,
		})
	}
}

type volumeListResponse struct {
	Volumes    []map[string]string
	Err        string
}

func serveVolumeList(f func() ([]map[string]string, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var volumes []map[string]string
		var err error
		volumes, err = f()
		log("\tdone: (%s, %v)\n", volumes, err)
		var errs string
		if err != nil {
			errs = err.Error()
		}
		json.NewEncoder(w).Encode(volumeListResponse{
			Volumes:    volumes,
			Err:        errs,
		})
	}
}

type driverCapabilitiesResponse struct {
	Capabilities    map[string]string
}

func serveDriverCapabilities(f func() map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		log("* %s\n", r.URL.String())
		var capabilties map[string]string
		capabilties = f()
		log("\tdone: (%s)\n", capabilties)
		json.NewEncoder(w).Encode(driverCapabilitiesResponse{
			Capabilities:    capabilties,
		})
	}
}
