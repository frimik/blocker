package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
)

type ebsVolumeDriver struct {
	ec2                 *ec2.EC2
	ec2meta             *ec2metadata.EC2Metadata
	awsInstanceId       string
	awsRegion           string
	awsAvailabilityZone string
}

func NewEbsVolumeDriver() (VolumeDriver, error) {
	d := &ebsVolumeDriver{}

	ec2sess := session.New()
	d.ec2meta = ec2metadata.New(ec2sess)

	// Fetch AWS information, validating along the way.
	if !d.ec2meta.Available() {
		return nil, errors.New("Not running on an EC2 instance.")
	}
	var err error
	if d.awsInstanceId, err = d.ec2meta.GetMetadata("instance-id"); err != nil {
		return nil, err
	}
	if d.awsRegion, err = d.ec2meta.Region(); err != nil {
		return nil, err
	}
	if d.awsAvailabilityZone, err =
		d.ec2meta.GetMetadata("placement/availability-zone"); err != nil {
		return nil, err
	}

	d.ec2 = ec2.New(ec2sess, &aws.Config{Region: aws.String(d.awsRegion)})

	// Print some diagnostic information and then return the driver.
	log("Auto-detected EC2 information:\n")
	log("\tInstanceId        : %v\n", d.awsInstanceId)
	log("\tRegion            : %v\n", d.awsRegion)
	log("\tAvailability Zone : %v\n", d.awsAvailabilityZone)
	return d, nil
}

func (d *ebsVolumeDriver) Create(name string, options map[string]string) error {
	/*
	var iops int64
	i, ok := options["iops"]
	if ok {
		iops, _ := strconv.ParseInt(i, 10, 64)
	}
	*/
	size, _ := strconv.ParseInt(options["size"], 10, 64)
	volume, err := d.ec2.CreateVolume(&ec2.CreateVolumeInput{
		AvailabilityZone: aws.String(d.awsAvailabilityZone),
		Size:             aws.Int64(size),
		//TODO support more options:
		/*
		SnapshotId:       options["snapshot-id"],
		VolumeType:       options["volume-type"],
		Iops:             &iops,
		KmsKeyId:         options["kms-key-id"],
		*/
	})
	if err != nil {
		return err
	}
	var volumeId = *volume.VolumeId

	d.ec2.CreateTags(&ec2.CreateTagsInput{
		Resources: []*string{aws.String(volumeId)},
		Tags:      []*ec2.Tag{
			&ec2.Tag{
				Key:   aws.String("Name"),
				Value: aws.String(name),
			},
		},
	})

	//format volume
	device, err := d.attachVolume(name)
	if err != nil {
		return err
	}
	//sudo?
	if out, err := exec.Command("mkfs", "-t", "ext4", device).CombinedOutput(); err != nil {
		// Make sure to detach the instance before quitting (ignoring errors).
		d.detachVolume(name)

		return fmt.Errorf("Formatting device %v failed: %v\n%v",
			device, err, string(out))
	}

	d.detachVolume(name)
	return nil
}

func (d *ebsVolumeDriver) Mount(path string) (string, error) {
	volume, folder := parsePath(path)
	mnt, err := d.doMount(volume)
	if err != nil {
		return "", err
	}
	return mnt + folder, nil
}

func (d *ebsVolumeDriver) Path(path string) (string, error) {
	volume, folder := parsePath(path)
	mnt := fmt.Sprintf("/mnt/blocker/%s%s", volume, folder)
	if stat, err := os.Stat(mnt); err != nil || !stat.IsDir() {
		return "", errors.New("Volume not mounted.")
	}
	return mnt, nil
}

func (d *ebsVolumeDriver) Remove(path string) error {
	volume, _ := parsePath(path)
	err := d.doUnmount(volume)
	if err != nil {
		return err
	}
	return nil
}

func (d *ebsVolumeDriver) Unmount(path string) error {
	volume, _ := parsePath(path)
	err := d.doUnmount(volume)
	if err != nil {
		return err
	}
	return nil
}

func (d *ebsVolumeDriver) Get(name string) (map[string]string, error) {
	ebs_volume, err := d.getEBSVolume(name)

	if err != nil {
		return nil, err
	}

	var volume = make(map[string]string)
	volume["Name"] = name
	volume["AwsVolumeId"] = *ebs_volume.VolumeId
	mount_path, err := d.Path(name)
	if err != nil {
		volume["Mountpoint"] = mount_path
	}
	return volume, nil
}

func (d *ebsVolumeDriver) List() ([]map[string]string, error) {
	info, err := d.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name: aws.String("tag-key"),
				Values: []*string{aws.String("Name")},
			},
		},
	})
	if err != nil {
		return nil, err
	}

	var volumes = make([]map[string]string, len(info.Volumes))
	for index := 0; index < len(info.Volumes); index++ {
		var VolumeId = *(info.Volumes[index].VolumeId)
		//TODO filter please
		var name = *(info.Volumes[index].Tags[0].Value)
		volumes[index] = make(map[string]string)
		volumes[index]["Name"] = name
		volumes[index]["AwsVolumeId"] = VolumeId
		mount_path, path_err := d.Path(name)
		if path_err != nil {
			volumes[index]["Mountpoint"] = mount_path
		}
	}
	return volumes, nil
}

func (d *ebsVolumeDriver) Capabilities() map[string]string {
	var capabilties = make(map[string]string)
	capabilties["Scope"] = "global"
	return capabilties
}

func (d *ebsVolumeDriver) getEBSVolume(name string) (*ec2.Volume, error) {
	volumes, err := d.ec2.DescribeVolumes(&ec2.DescribeVolumesInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name: aws.String("tag:Name"),
				Values: []*string{aws.String(name)},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(volumes.Volumes) == 0 {
		return nil, errors.New("Volume not found")
	}
	return volumes.Volumes[0], nil
}

func parsePath(path string) (string, string) {
	sep := strings.Index(path, "/")
	if sep < 0 {
		return path, ""
	}
	return path[:sep], path[sep:]
}

func (d *ebsVolumeDriver) doMount(name string) (string, error) {
	// Auto-generate a random mountpoint.
	mnt := "/mnt/blocker/" + name

	// Ensure the directory /mnt/blocker/<m> exists.
	if err := os.MkdirAll(mnt, os.ModeDir|0700); err != nil {
		return "", err
	}
	if stat, err := os.Stat(mnt); err != nil || !stat.IsDir() {
		return "", fmt.Errorf("Mountpoint %v is not a directory: %v", mnt, err)
	}

	if err := exec.Command("mountpoint", "-q", mnt).Run(); err == nil {
		return mnt, nil
	}

	// Attach the EBS device to the current EC2 instance.
	dev, err := d.attachVolume(name)
	if err != nil {
		return "", err
	}

	// Now go ahead and mount the EBS device to the desired mountpoint.
	// TODO: support encrypted filesystems.
	if out, err := exec.Command("mount", dev, mnt).CombinedOutput(); err != nil {
		// Make sure to detach the instance before quitting (ignoring errors).
		d.detachVolume(name)

		return "", fmt.Errorf("Mounting device %v to %v failed: %v\n%v",
			dev, mnt, err, string(out))
	}

	// And finally set and return it.
	return mnt, nil
}

func (d *ebsVolumeDriver) waitUntilState(
	name string, check func(*ec2.Volume) error) error {
	// Most volume operations are asynchronous, and we often need to wait until
	// state transitions finish before proceeding to the mount.  Sadly, this
	// requires some clunky retries, sleeps, and that kind of crap.
	tries := 0
	for {
		tries++

		volume, err := d.getEBSVolume(name)
		if err != nil {
			return err
		}

		// Check to see if the volume reached the intended state; if yes, return.
		err = check(volume)
		if err == nil {
			return nil
		}
		if tries == 12 {
			return err
		}

		log("\tWaiting for EBS attach to complete...\n")
		time.Sleep(5 * time.Second)
	}

	return nil
}

func (d *ebsVolumeDriver) waitUntilAttached(name string) error {
	return d.waitUntilState(name, func(volume *ec2.Volume) error {
		var attachment *ec2.VolumeAttachment
		if len(volume.Attachments) == 1 {
			attachment = volume.Attachments[0]
			if *attachment.State == ec2.VolumeAttachmentStateAttached {
				return nil
			}
		}
		if attachment == nil {
			return fmt.Errorf(
				"Volume state transition failed: expected 1 attachment, got %v",
				len(volume.Attachments))
		} else {
			return fmt.Errorf(
				"Volume state transition failed: seeking %v, current is %v",
				ec2.VolumeAttachmentStateAttached, *attachment.State)
		}
	})
}

func (d *ebsVolumeDriver) waitUntilDettached(name string) error {
	return d.waitUntilState(name, func(volume *ec2.Volume) error {
		if len(volume.Attachments) == 0 {
			return nil
		}
		return fmt.Errorf(
			"Volume state transition failed: still has attachments")
	})
}

func (d *ebsVolumeDriver) waitUntilAvailable(name string) error {
	return d.waitUntilState(name, func(volume *ec2.Volume) error {
		if *volume.State == ec2.VolumeStateAvailable {
			return nil
		}
		return fmt.Errorf(
			"Volume state transition failed: seeking %v, current is %v",
			ec2.VolumeStateAvailable, *volume.State)
	})
}

func (d *ebsVolumeDriver) attachVolume(name string) (string, error) {
	// Check if the volume is already attached to instance
	volume, err := d.getEBSVolume(name)
	if err != nil {
		return "", err
	}
	if len(volume.Attachments) == 1 {
		if *volume.Attachments[0].State == ec2.VolumeAttachmentStateAttached &&
			*volume.Attachments[0].InstanceId == d.awsInstanceId {
			re := regexp.MustCompile("/dev/(xv|s)d([f-p])")
			res := re.FindStringSubmatch(*volume.Attachments[0].Device)
			if len(res) != 3 {
				return "", errors.New("Unable to find mount device for " + name)
			}
			if _, err := os.Lstat("/dev/sd" + res[2]); err == nil {
				return "/dev/sd" + res[2], nil
			}
			if _, err := os.Lstat("/dev/xvd" + res[2]); err == nil {
				return "/dev/xvd" + res[2], nil
			}
		}
	}

	// Since detaching is asynchronous, we want to check first to see if the
	// target volume is in the process of being detached.  If it is, we'll wait
	// a little bit until it's ready to use.
	err = d.waitUntilAvailable(name)
	if err != nil {
		return "", err
	}

	// Now find the first free device to attach the EBS volume to.  See
	// http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/device_naming.html
	// for recommended naming scheme (/dev/sd[f-p]).
	for _, c := range "fghijklmnop" {
		dev := "/dev/sd" + string(c)
		altdev := "/dev/xvd" + string(c)

		if _, err := os.Lstat(dev); err == nil {
			continue
		}
		if _, err := os.Lstat(altdev); err == nil {
			continue
		}

		if _, err := d.ec2.AttachVolume(&ec2.AttachVolumeInput{
			Device:     aws.String(dev),
			InstanceId: aws.String(d.awsInstanceId),
			VolumeId:   volume.VolumeId,
		}); err != nil {
			if awsErr, ok := err.(awserr.Error); ok &&
				awsErr.Code() == "InvalidParameterValue" {
				// If AWS is simply reporting that the device is already in
				// use, then go ahead and check the next one.
				continue
			}

			return "", err
		}

		err = d.waitUntilAttached(name)
		if err != nil {
			return "", err
		}

		// Finally, the attach is complete.
		log("\tAttached EBS volume %v to %v:%v.\n", name, d.awsInstanceId, dev)
		if _, err := os.Lstat(dev); os.IsNotExist(err) {
			// On newer Linux kernels, /dev/sd* is mapped to /dev/xvd*.  See
			// if that's the case.
			if _, err := os.Lstat(altdev); os.IsNotExist(err) {
				d.detachVolume(name)
				return "", fmt.Errorf("Device %v is missing after attach.", dev)
			}

			log("\tLocal device name is %v\n", altdev)
			dev = altdev
		}

		return dev, nil
	}

	return "", errors.New("No devices available for attach: /dev/sd[f-p] taken.")
}

func (d *ebsVolumeDriver) doUnmount(name string) error {
	mnt := "/mnt/blocker/" + name

	// First unmount the device.
	if out, err := exec.Command("umount", mnt).CombinedOutput(); err != nil {
		return fmt.Errorf("Unmounting %v failed: %v\n%v", mnt, err, string(out))
	}

	// Remove the mountpoint from the filesystem.
	if err := os.Remove(mnt); err != nil {
		return err
	}

	// Detach the EBS volume from this AWS instance.
	if err := d.detachVolume(name); err != nil {
		return err
	}

	// Finally clear out the slot and return.
	return nil
}

func (d *ebsVolumeDriver) detachVolume(name string) error {
	volume, err := d.getEBSVolume(name)
	if err != nil {
		return err
	}
	if _, err := d.ec2.DetachVolume(&ec2.DetachVolumeInput{
		InstanceId: aws.String(d.awsInstanceId),
		VolumeId:   volume.VolumeId,
	}); err != nil {
		return err
	}

	err = d.waitUntilDettached(name)
	if err != nil {
		return err
	}

	log("\tDetached EBS volume %v from %v.\n", name, d.awsInstanceId)
	return nil
}
