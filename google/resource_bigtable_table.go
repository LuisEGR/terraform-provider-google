package google

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/structure"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
)

func resourceBigtableTable() *schema.Resource {
	return &schema.Resource{
		Create:        resourceBigtableTableCreate,
		Read:          resourceBigtableTableRead,
		Update:        resourceBigtableTableUpdate,
		Delete:        resourceBigtableTableDestroy,
		CustomizeDiff: resourceBigtableFamilyGCPolicyCustomizeDiff,

		Importer: &schema.ResourceImporter{
			State: resourceBigtableTableImport,
		},

		// Set a longer timeout for table creation as adding column families can be slow.
		Timeouts: &schema.ResourceTimeout{
			Create: schema.DefaultTimeout(45 * time.Minute),
		},

		// ----------------------------------------------------------------------
		// IMPORTANT: Do not add any additional ForceNew fields to this resource.
		// Destroying/recreating tables can lead to data loss for users.
		// ----------------------------------------------------------------------
		Schema: map[string]*schema.Schema{
			"name": {
				Type:        schema.TypeString,
				Required:    true,
				ForceNew:    false,
				Description: `The name of the table.`,
			},

			"column_family": {
				Type:        schema.TypeSet,
				Optional:    true,
				ForceNew:    false,
				Description: `A group of columns within a table which share a common configuration. This can be specified multiple times.`,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"family": {
							Type:        schema.TypeString,
							Required:    true,
							Description: `The name of the column family.`,
						},

						"gc_rule": {
							Type:     schema.TypeSet,
							Optional: true,
							ForceNew: false,
							// MaxItems:    1,
							Description: `The garbage collection rule which deletes cells in a column older than the given age.`,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"gc_rules": {
										Type:         schema.TypeString,
										Optional:     true,
										ForceNew:     false,
										Description:  `Serialized JSON string for garbage collection policy. Conflicts with "mode", "max_age" and "max_version".`,
										ValidateFunc: validation.StringIsJSON,
										// ConflictsWith: []string{"mode", "max_age", "max_version"},
										StateFunc: func(v interface{}) string {
											json, _ := structure.NormalizeJsonString(v)
											return json
										},
									},
									"mode": {
										Type:         schema.TypeString,
										Optional:     true,
										ForceNew:     false,
										Description:  `If multiple policies are set, you should choose between UNION OR INTERSECTION.`,
										ValidateFunc: validation.StringInSlice([]string{GCPolicyModeIntersection, GCPolicyModeUnion}, false),
										// ConflictsWith: []string{"column_family.#.gc_rule.gc_rules"},
									},
									"max_age": {
										Type:        schema.TypeSet,
										Optional:    true,
										ForceNew:    false,
										Description: `GC policy that applies to all cells older than the given age.`,
										MaxItems:    1,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"days": {
													Type:        schema.TypeInt,
													Optional:    true,
													Computed:    true,
													ForceNew:    false,
													Deprecated:  "Deprecated in favor of duration",
													Description: `Number of days before applying GC policy.`,
													// ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
												},
												"duration": {
													Type:         schema.TypeString,
													Optional:     true,
													Computed:     true,
													ForceNew:     false,
													Description:  `Duration before applying GC policy`,
													ValidateFunc: validateDuration(),
													// ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
												},
											},
										},
										// ConflictsWith: []string{"column_family.#.gc_rule.gc_rules"},
									},
									"max_version": {
										Type:        schema.TypeList,
										Optional:    true,
										ForceNew:    false,
										Description: `GC policy that applies to all versions of a cell except for the most recent.`,
										Elem: &schema.Resource{
											Schema: map[string]*schema.Schema{
												"number": {
													Type:        schema.TypeInt,
													Required:    true,
													ForceNew:    false,
													Description: `Number of version before applying the GC policy.`,
												},
											},
										},
										// ConflictsWith: []string{"column_family.#.gc_rule.gc_rules"},
									},
								},
							},
						},

						// "gc_rule": map[string]*schema.Schema{
						// 	"mode": {
						// 		Type:          schema.TypeString,
						// 		Optional:      true,
						// 		ForceNew:      true,
						// 		Description:   `If multiple policies are set, you should choose between UNION OR INTERSECTION.`,
						// 		ValidateFunc:  validation.StringInSlice([]string{GCPolicyModeIntersection, GCPolicyModeUnion}, false),
						// 		ConflictsWith: []string{"gc_rules"},
						// 	},
						// },
					},
				},
			},

			"instance_name": {
				Type:             schema.TypeString,
				Required:         true,
				ForceNew:         true,
				DiffSuppressFunc: compareResourceNames,
				Description:      `The name of the Bigtable instance.`,
			},

			"split_keys": {
				Type:        schema.TypeList,
				Optional:    true,
				ForceNew:    true,
				Elem:        &schema.Schema{Type: schema.TypeString},
				Description: `A list of predefined keys to split the table on. !> Warning: Modifying the split_keys of an existing table will cause Terraform to delete/recreate the entire google_bigtable_table resource.`,
			},

			"project": {
				Type:        schema.TypeString,
				Optional:    true,
				Computed:    true,
				ForceNew:    true,
				Description: `The ID of the project in which the resource belongs. If it is not provided, the provider project is used.`,
			},
		},
		UseJSONNumber: true,
	}
}

func resourceBigtableTableCreate(d *schema.ResourceData, meta interface{}) error {
	fmt.Println(">>>>>>>>>>>>>> d :", d)
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}

	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}
	if err := d.Set("instance_name", instanceName); err != nil {
		return fmt.Errorf("Error setting instance_name: %s", err)
	}

	defer c.Close()

	tableId := d.Get("name").(string)
	tblConf := bigtable.TableConf{TableID: tableId}

	// Set the split keys if given.
	if v, ok := d.GetOk("split_keys"); ok {
		tblConf.SplitKeys = convertStringArr(v.([]interface{}))
	}

	// Set the column families if given.
	columnFamilies := make(map[string]bigtable.GCPolicy)
	fmt.Println(">>>>>>>>>>>>>>>>>> TypeOf columnFamilies :", fmt.Sprintf("%T", columnFamilies))
	if d.Get("column_family.#").(int) > 0 {
		// columns := d.Get("column_family").(*schema.Set).List()
		columns := d.Get("column_family").([]interface{})

		fmt.Println(">>>>>>>>>>>>>>>>>> TypeOf columns :", fmt.Sprintf("%T", columns))
		fmt.Println(">>>>>>>c olumns :", columns)
		for _, co := range columns {
			column := co.(map[string]interface{})

			// gc_rule := schema.ResourceData{"schema": column["gc_rule"]}

			gc_rule, ok := column["gc_rule"]

			// f, ok := baz.(*foo)
			if !ok {
				fmt.Println("gc_rule1:", gc_rule)
				// baz was not of type *foo. The assertion failed
			}
			fmt.Println("gc_rule2:", gc_rule)
			// fmt.Println("gc_rule3:", gc_rule))
			fmt.Println("Typeof gc_rule:", reflect.TypeOf(gc_rule))

			// for _, item := range gc_rule.([]interface{}) {
			// 	fmt.Println("item :", item)
			// 	fmt.Printf("Ittttm, %v", item.(map[string]interface{}))
			// }

			gcPolicy, err := generateBigtableFamilyGCPolicy(gc_rule.([]interface{})[0].(map[string]interface{}))
			fmt.Println(">>>>>>>>>>>>> GENERATED gcPolicy :", gcPolicy)
			if err != nil {
				return err
			}

			// gc_rule["mode"]
			// fmt.Println("gc_rule[mode]  :", gc_rule["mode"] );
			// Print the column gc_rule content
			// for _, gc := range gc_rule {
			// 	gc := gc.(map[string]interface{})
			// 	fmt.Println("gc :", gc)
			// 	fmt.Println("gc[mode] :", gc["mode"])
			// 	fmt.Println("gc[gc_rules] :", gc["gc_rules"])
			// 	// fmt.Println("gc[max_age] :", gc["max_age"]);
			// 	// fmt.Println("gc[max_version] :", gc["max_version"]);
			// 	// fmt.Println("gc[intersection] :", gc["intersection"])
			// }

			if v, ok := column["family"]; ok {
				// By default, there is no GC rules.
				// columnFamilies[v.(string)] = bigtable.NoGcPolicy()

				// Here we can add the GC rules to avoid having them separated
				fmt.Println("v.(string) :", v.(string))
				fmt.Println("ok :", ok)

				columnFamilies[v.(string)] = gcPolicy

			}
		}
	}
	tblConf.Families = columnFamilies

	// This method may return before the table's creation is complete - we may need to wait until
	// it exists in the future.
	// Set a longer timeout as creating table and adding column families can be pretty slow.
	ctxWithTimeout, cancel := context.WithTimeout(ctx, 20*time.Minute)
	defer cancel() // Always call cancel.
	err = c.CreateTableFromConf(ctxWithTimeout, &tblConf)
	if err != nil {
		return fmt.Errorf("Error creating table. %s", err)
	}

	id, err := replaceVars(d, config, "projects/{{project}}/instances/{{instance_name}}/tables/{{name}}")
	if err != nil {
		return fmt.Errorf("Error constructing id: %s", err)
	}
	d.SetId(id)

	return resourceBigtableTableRead(d, meta)
}

func resourceBigtableTableRead(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}
	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}

	defer c.Close()

	name := d.Get("name").(string)
	table, err := c.TableInfo(ctx, name)
	fmt.Println(">>>>>>>>resourceBigtableTableRead-table :", table)
	if err != nil {
		log.Printf("[WARN] Removing %s because it's gone", name)
		d.SetId("")
		return nil
	}

	if err := d.Set("project", project); err != nil {
		return fmt.Errorf("Error setting project: %s", err)
	}
	fmt.Println("table.Families1 :", table.Families)
	fmt.Println("table.Families1Flatten :", flattenColumnFamily(table.Families))
	fmt.Println("table.Families2:", table.FamilyInfos)
	fmt.Println("table.Families2-flatten:", flattenColumnFamily2(table.FamilyInfos))
	if err := d.Set("column_family", flattenColumnFamily2(table.FamilyInfos)); err != nil {
		// if err := d.Set("column_family", flattenColumnFamily(table.Families)); err != nil {
		return fmt.Errorf("Error setting column_family: %s", err)
	}

	return nil
}

func resourceBigtableTableUpdate(d *schema.ResourceData, meta interface{}) error {
	fmt.Println(">>>> resourceBigtableTableUpdate :", d)
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}
	defer c.Close()

	o, n := d.GetChange("column_family")
	oSet := o.(*schema.Set)
	fmt.Println("oSet :", oSet)
	fmt.Printf("oSet: %#v", oSet)

	nSet := n.(*schema.Set)
	fmt.Println("nSet :", nSet)
	fmt.Printf("nSet1: %#v", nSet)

	// diffColumnFamilies(oSet, nSet)

	// deletedCF := getDeletedColumnFamilies(oSet, nSet)
	// fmt.Printf("deletedCF: %#v", deletedCF)

	// created, updated := getUpdatedAndCreatedColumnFamilies(oSet, nSet)
	// fmt.Printf("createdCF: %#v", created)
	// fmt.Printf("updatedCF: %#v", updated)

	// fmt.Printf("nSet2: %+v", nSet)
	name := d.Get("name").(string)

	// set1 := *schema.Set(map[string]interface{}{"1997192000": map[string]interface{}{"family": "name", "gc_rule": []interface{}{map[string]interface{}{"gc_rules": "", "max_age": []interface{}{map[string]interface{}{"days": 0, "duration": "120h"}}, "max_version": []interface{}{map[string]interface{}{"number": 10}}, "mode": "UNION"}}}})
	// set2 := *schema.Set(map[string]interface{}{"1997192000": map[string]interface{}{"family": "name", "gc_rule": []interface{}{map[string]interface{}{"gc_rules": "", "max_age": []interface{}{map[string]interface{}{"days": 0, "duration": "120h"}}, "max_version": []interface{}{map[string]interface{}{"number": 10}}, "mode": "UNION"}}}})
	// fmt.Printf("set1set2.Differe: %#v", set1.Difference(set2).List())

	// fmt.Println("nSet.Difference(oSet) :", nSet.Difference(oSet))
	// fmt.Printf("nSet.Differe: %#v", nSet.Difference(oSet).List())

	// Add new Column families
	// for _, cf := range created {
	// 	cfname := getCFName(cf)
	// 	if err := c.CreateColumnFamily(ctx, name, cfname); err != nil {
	// 		return fmt.Errorf("Error creating column family %q: %s", cfname, err)
	// 	}
	// }

	toremove := oSet.Difference(nSet).List()
	fmt.Printf("toremove : %#v", toremove)

	// Remove column families that are in old but not in new
	for _, old := range oSet.Difference(nSet).List() {
		column := old.(map[string]interface{})

		if v, ok := column["family"]; ok {
			log.Printf("[DEBUG] removing column family %q", v)
			if err := c.DeleteColumnFamily(ctx, name, v.(string)); err != nil {
				return fmt.Errorf("Error deleting column family %q: %s", v, err)
			}
		}
	}

	// Add column families that are in new but not in old
	newones := nSet.Difference(oSet).List()
	fmt.Printf("newones : %#v", newones)
	//   fmt.Println(`ewones : ", `, ewones : %#v );
	for _, new := range nSet.Difference(oSet).List() {
		column := new.(map[string]interface{})

		fmt.Println("ADDING column[family] :", column["family"])
		if v, ok := column["family"]; ok {
			log.Printf("[DEBUG] adding column family %q", v)
			if err := c.CreateColumnFamily(ctx, name, v.(string)); err != nil {
				return fmt.Errorf("Error creating column family %q: %s", v, err)
			}

			// Add GCRule
			if v, ok := column["gc_rule"]; ok {
				fmt.Printf("ADDING column[gc_rule] : %#v", v)
				gcpolicy, _ := generateBigtableFamilyGCPolicy(v.([]interface{})[0].(map[string]interface{}))
				if err := c.SetGCPolicy(ctx, name, column["family"].(string), gcpolicy); err != nil {
					return fmt.Errorf("Error setting GCPolicy %q: %s", v, err)
				}
			}
		}
	}

	// toremove := oSet.Difference(nSet).List()
	// fmt.Printf("toremove : %#v", toremove)

	return resourceBigtableTableRead(d, meta)
}

// func diffColumnFamilies(oldCF []interface{}, newCF []interface{}) {

// 	// oldMap := make(map[string]interface{})
// 	maxItems := len(oldCF) + len(newCF)

// 	newColumns := make([]map[string]interface{}, 0, maxItems)

// 	for _, old := range oldCF {
// 		oldFam := old.(map[string]interface{})["family"]
// 		fmt.Println("oldFAM :", old.(map[string]interface{})["family"])

// 		for _, new := range newCF {
// 			newFam := new.(map[string]interface{})["family"]
// 			fmt.Println("newFAM :", new.(map[string]interface{})["family"])

// 			if newFam == oldFam {
// 				fmt.Println("newFAM == oldFAM")
// 				equal := reflect.DeepEqual(old, new)
// 				fmt.Println("equal :", equal)
// 				if !equal {
// 					fmt.Println("old != new")
// 					newColumns = append(newColumns, new.(map[string]interface{}))
// 				}
// 			}
// 		}

// 	}

// 	// return oldSet.Difference(newSet).List(), newSet.Difference(oldSet).List()
// }

// func getCFName(cf interface{}) string {
// 	return cf.(map[string]interface{})["family"].(string)
// }

// func getDeletedColumnFamilies(oldCF []interface{}, newCF []interface{}) []interface{} {
// 	deleted := make([]interface{}, 0, len(oldCF))

// 	for _, old := range oldCF {
// 		oldFam := old.(map[string]interface{})["family"]
// 		fmt.Println("oldFAM :", old.(map[string]interface{})["family"])

// 		found := false
// 		for _, new := range newCF {
// 			newFam := new.(map[string]interface{})["family"]
// 			fmt.Println("newFAM :", new.(map[string]interface{})["family"])

// 			if newFam == oldFam {
// 				found = true
// 				break
// 			}
// 		}

// 		if !found {
// 			deleted = append(deleted, old)
// 		}
// 	}

// 	return deleted
// }

// func getUpdatedAndCreatedColumnFamilies(oldCF []interface{}, newCF []interface{}) ([]interface{}, []interface{}) {
// 	updated := make([]interface{}, 0, len(newCF))
// 	created := make([]interface{}, 0, len(newCF))

// 	for _, new := range newCF {
// 		newFam := new.(map[string]interface{})["family"]
// 		fmt.Println("newFam :", new.(map[string]interface{})["family"])

// 		found := false
// 		isUpdated := false
// 		for _, old := range oldCF {
// 			oldFam := old.(map[string]interface{})["family"]
// 			fmt.Println("oldFam :", old.(map[string]interface{})["family"])

// 			if newFam == oldFam {
// 				equal := reflect.DeepEqual(old, new)
// 				found = true
// 				if !equal {
// 					isUpdated = true
// 				}
// 				break
// 			}
// 		}

// 		if found && isUpdated {
// 			updated = append(updated, new)
// 		}

// 		if !found {
// 			created = append(created, new)
// 		}
// 	}

// 	return created, updated
// }

func resourceBigtableTableDestroy(d *schema.ResourceData, meta interface{}) error {
	config := meta.(*Config)
	userAgent, err := generateUserAgentString(d, config.userAgent)
	if err != nil {
		return err
	}

	ctx := context.Background()

	project, err := getProject(d, config)
	if err != nil {
		return err
	}

	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
	if err != nil {
		return fmt.Errorf("Error starting admin client. %s", err)
	}

	defer c.Close()

	name := d.Get("name").(string)
	err = c.DeleteTable(ctx, name)
	if err != nil {
		return fmt.Errorf("Error deleting table. %s", err)
	}

	d.SetId("")

	return nil
}

func flattenColumnFamily(families []string) []map[string]interface{} {
	fmt.Println("flattenColumnFamily :", families)
	result := make([]map[string]interface{}, 0, len(families))

	for _, f := range families {
		data := make(map[string]interface{})
		data["family"] = f
		// data["gc_rule"] = []interface{}
		result = append(result, data)
	}

	fmt.Println("flattenColumnFamily-result :", result)
	return result
}

func flattenColumnFamily2(families []bigtable.FamilyInfo) []map[string]interface{} {
	fmt.Println("flattenColumnFamily2 :", families)
	result := make([]map[string]interface{}, 0, len(families))

	for _, v := range families {
		data := make(map[string]interface{})
		data["family"] = v.Name
		data["gc_rule"], _ = flattenGCRule(v.FullGCPolicy)
		result = append(result, data)
	}

	fmt.Println("flattenColumnFamily2-result :", result)
	return result
}

func flattenGCRule(rule bigtable.GCPolicy) ([]map[string]interface{}, error) {
	fmt.Println("flattenGCRule :", rule)
	fmt.Println("typeof flattenGCRule :", reflect.TypeOf(rule))

	gcs, err := gcPolicyToGCRuleString(rule, false)
	if err != nil {
		return nil, err
	}
	fmt.Println("gcs :", gcs) //map[mode:union rules:[map[max_age:7d] map[max_version:10]]]
	fmt.Println("typeof gcs :", reflect.TypeOf(gcs))

	result := make([]map[string]interface{}, 0, 1)

	data := make(map[string]interface{})
	fmt.Println("gcs[mode] :", gcs["mode"])

	if v, ok := gcs["mode"]; ok {
		data["mode"] = v
	}

	if v, ok := gcs["max_version"]; ok {
		data["max_version"] = []interface{}{map[string]interface{}{"number": v}}
	}

	if v, ok := gcs["max_age"]; ok {
		data["max_age"] = []interface{}{map[string]interface{}{"duration": v}}
	}
	// data["mode"] = gcs["mode"].(string)

	// policyFromJSON, e := getGCPolicyFromJSON(gcs, false)
	// fmt.Println("e :", e)
	// fmt.Println("policyFromJSON :", policyFromJSON)

	// data["max_age"] = make(map[string]interface{})

	if v, ok := gcs["rules"]; ok {

		durationMaxAge, _ := getMaxAgeDuration2(v.([]interface{})[0].(map[string]interface{})["max_age"].(string))

		data["max_age"] = make([]map[string]interface{}, 0, 1)
		data["max_age"] = append(data["max_age"].([]map[string]interface{}), map[string]interface{}{
			"duration": durationMaxAge.String(),
		})

		data["max_version"] = make([]map[string]interface{}, 0, 1)
		data["max_version"] = append(data["max_version"].([]map[string]interface{}), map[string]interface{}{
			"number": v.([]interface{})[1].(map[string]interface{})["max_version"].(float64),
		})

	}

	// data["max_age"] = []map[string]interface{}{
	// 	"duration": durationMaxAge,
	// }

	// ["duration"], _ = getMaxAgeDuration2(gcs["rules"].([]interface{})[0].(map[string]interface{})["max_age"])
	// data["max_version"] = gcs["max_version"][1]

	// data["max_age"] = rule.MaxAge
	// data["max_versions"] = rule.MaxVersions
	// data["intersection"] = flattenGCRule(rule.Intersection)
	// data["union"] = flattenGCRule(rule.Union)
	data["mode"] = gcs["mode"]
	result = append(result, data)

	fmt.Println("flattenGCRule-result :", result)
	return result, nil
}

// TODO(rileykarson): Fix the stored import format after rebasing 3.0.0
func resourceBigtableTableImport(d *schema.ResourceData, meta interface{}) ([]*schema.ResourceData, error) {
	config := meta.(*Config)
	if err := parseImportId([]string{
		"projects/(?P<project>[^/]+)/instances/(?P<instance_name>[^/]+)/tables/(?P<name>[^/]+)",
		"(?P<project>[^/]+)/(?P<instance_name>[^/]+)/(?P<name>[^/]+)",
		"(?P<instance_name>[^/]+)/(?P<name>[^/]+)",
	}, d, config); err != nil {
		return nil, err
	}

	// Replace import id for the resource id
	id, err := replaceVars(d, config, "projects/{{project}}/instances/{{instance_name}}/tables/{{name}}")
	if err != nil {
		return nil, fmt.Errorf("Error constructing id: %s", err)
	}
	d.SetId(id)

	return []*schema.ResourceData{d}, nil
}

func resourceBigtableFamilyGCPolicyCustomizeDiffFunc(diff TerraformResourceDiff) error {
	log.Printf(">>>>>>>>>diff : %#v, column_family: %#v", diff, diff.Get("column_family"))
	log.Printf("nameDIFF :%#v", diff.Get("name"))

	hasChange := diff.HasChange("column_family")
	fmt.Println("hasChange :", hasChange)

	// fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> diff :", diff)
	columnFamilies := diff.Get("column_family")
	log.Printf("column_familyDIFF :%#v", columnFamilies)

	old, new := diff.GetChange("column_family")

	log.Printf("old :%#v", old)
	log.Printf("new :%#v", new)

	// Comparing the old and new duration values for each column family
	for idx, oldCF := range old.(*schema.Set).List() {
		fmt.Println("idx1 :", idx)

		oldCFMap := oldCF.(map[string]interface{})
		oldCFName := oldCFMap["family"].(string)

		oldMaxAgeRules := oldCFMap["gc_rule"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].(*schema.Set).List()
		//   fmt.Println("oldCFMap["gc_rule"] :", oldCFMap["gc_rule"]);

		if len(oldMaxAgeRules) > 0 {

			oldCFDuration := oldMaxAgeRules[0].(map[string]interface{})["duration"].(string)
			oldCFDurationTime, _ := getMaxAgeDuration2(oldCFDuration)
			fmt.Println("oldCFDuration :", oldCFDuration)
			fmt.Println("oldCFDurationTime :", oldCFDurationTime)

			for _, newCF := range new.(*schema.Set).List() {
				fmt.Printf("newCF : %#v", newCF)
				newCFMap := newCF.(map[string]interface{})
				newCFName := newCFMap["family"].(string)

				fmt.Printf("newCFMap[gc_rule] :%#v", newCFMap["gc_rule"])
				newMaxAgeRules := newCFMap["gc_rule"].(*schema.Set).List()[0].(map[string]interface{})["max_age"].(*schema.Set).List()
				if len(newMaxAgeRules) > 0 {

					newCFDuration := newMaxAgeRules[0].(map[string]interface{})["duration"].(string)
					newCFDurationTime, _ := getMaxAgeDuration2(newCFDuration)
					fmt.Println("newCFDuration :", newCFDuration)
					fmt.Println("newCFDurationTime :", newCFDurationTime)

					if oldCFName == newCFName && oldCFDurationTime == newCFDurationTime {
						diffUpdate := fmt.Sprintf("column_family.%d.gc_rule.0.max_age.0.duration", idx)
						err := diff.Clear(diffUpdate)
						if err != nil {
							return err
						}
						// return fmt.Errorf("Cannot change the duration of an existing column family")
					}
				}
			}
		}
	}

	// log.Printf("column_familyDIFFChange :%#v", columnFamiliesChange)
	// if count < 1 {
	// 	return nil
	// }

	// oldDays, newDays := diff.GetChange("column_family.gc_rule.gc_rules.max_age.0.days")
	// oldDuration, newDuration := diff.GetChange("column_family.gc_rule.gc_rules.max_age.0.duration")
	// log.Printf("days: %v %v", oldDays, newDays)
	// log.Printf("duration: %v %v", oldDuration, newDuration)

	// if oldDuration == "" && newDuration != "" {
	// 	// flatten the old days and the new duration to duration... if they are
	// 	// equal then do nothing.
	// 	do, err := time.ParseDuration(newDuration.(string))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	dn := time.Hour * 24 * time.Duration(oldDays.(int))
	// 	if do == dn {
	// 		err := diff.Clear("column_family.gc_rule.gc_rules.max_age.0.days")
	// 		if err != nil {
	// 			return err
	// 		}
	// 		err = diff.Clear("column_family.gc_rule.gc_rules.max_age.0.duration")
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// }

	return nil
}

func resourceBigtableFamilyGCPolicyCustomizeDiff(_ context.Context, d *schema.ResourceDiff, meta interface{}) error {
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>> resourceBigtableFamilyGCPolicyCustomizeDiff :", d)

	return resourceBigtableFamilyGCPolicyCustomizeDiffFunc(d)
}

// func resourceBigtableGCPolicy() *schema.Resource {
// 	return &schema.Resource{
// 		// Create:        resourceBigtableGCPolicyUpsert,
// 		// Read:          resourceBigtableGCPolicyRead,
// 		// Delete:        resourceBigtableGCPolicyDestroy,
// 		// Update:        resourceBigtableGCPolicyUpsert,
// 		// CustomizeDiff: resourceBigtableGCPolicyCustomizeDiff,

// 		Schema: map[string]*schema.Schema{
// 			"instance_name": {
// 				Type:             schema.TypeString,
// 				Required:         true,
// 				ForceNew:         true,
// 				DiffSuppressFunc: compareResourceNames,
// 				Description:      `The name of the Bigtable instance.`,
// 			},

// 			"table": {
// 				Type:        schema.TypeString,
// 				Required:    true,
// 				ForceNew:    true,
// 				Description: `The name of the table.`,
// 			},

// 			"column_family": {
// 				Type:        schema.TypeString,
// 				Required:    true,
// 				ForceNew:    true,
// 				Description: `The name of the column family.`,
// 			},

// 			"gc_rules": {
// 				Type:          schema.TypeString,
// 				Optional:      true,
// 				Description:   `Serialized JSON string for garbage collection policy. Conflicts with "mode", "max_age" and "max_version".`,
// 				ValidateFunc:  validation.StringIsJSON,
// 				ConflictsWith: []string{"mode", "max_age", "max_version"},
// 				StateFunc: func(v interface{}) string {
// 					json, _ := structure.NormalizeJsonString(v)
// 					return json
// 				},
// 			},
// 			"mode": {
// 				Type:          schema.TypeString,
// 				Optional:      true,
// 				ForceNew:      true,
// 				Description:   `If multiple policies are set, you should choose between UNION OR INTERSECTION.`,
// 				ValidateFunc:  validation.StringInSlice([]string{GCPolicyModeIntersection, GCPolicyModeUnion}, false),
// 				ConflictsWith: []string{"gc_rules"},
// 			},

// 			"max_age": {
// 				Type:        schema.TypeList,
// 				Optional:    true,
// 				ForceNew:    true,
// 				Description: `GC policy that applies to all cells older than the given age.`,
// 				MaxItems:    1,
// 				Elem: &schema.Resource{
// 					Schema: map[string]*schema.Schema{
// 						"days": {
// 							Type:         schema.TypeInt,
// 							Optional:     true,
// 							Computed:     true,
// 							ForceNew:     true,
// 							Deprecated:   "Deprecated in favor of duration",
// 							Description:  `Number of days before applying GC policy.`,
// 							ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
// 						},
// 						"duration": {
// 							Type:         schema.TypeString,
// 							Optional:     true,
// 							Computed:     true,
// 							ForceNew:     true,
// 							Description:  `Duration before applying GC policy`,
// 							ValidateFunc: validateDuration(),
// 							ExactlyOneOf: []string{"max_age.0.days", "max_age.0.duration"},
// 						},
// 					},
// 				},
// 				ConflictsWith: []string{"gc_rules"},
// 			},

// 			"max_version": {
// 				Type:        schema.TypeList,
// 				Optional:    true,
// 				ForceNew:    true,
// 				Description: `GC policy that applies to all versions of a cell except for the most recent.`,
// 				Elem: &schema.Resource{
// 					Schema: map[string]*schema.Schema{
// 						"number": {
// 							Type:        schema.TypeInt,
// 							Required:    true,
// 							ForceNew:    true,
// 							Description: `Number of version before applying the GC policy.`,
// 						},
// 					},
// 				},
// 				ConflictsWith: []string{"gc_rules"},
// 			},

// 			"project": {
// 				Type:        schema.TypeString,
// 				Optional:    true,
// 				Computed:    true,
// 				ForceNew:    true,
// 				Description: `The ID of the project in which the resource belongs. If it is not provided, the provider project is used.`,
// 			},
// 		},
// 		UseJSONNumber: true,
// 	}
// }

// func resourceBigtableGCPolicyUpsert(d *schema.ResourceData, meta interface{}) error {
// 	config := meta.(*Config)
// 	userAgent, err := generateUserAgentString(d, config.userAgent)
// 	if err != nil {
// 		return err
// 	}

// 	ctx := context.Background()

// 	project, err := getProject(d, config)
// 	if err != nil {
// 		return err
// 	}

// 	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
// 	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
// 	if err != nil {
// 		return fmt.Errorf("Error starting admin client. %s", err)
// 	}
// 	if err := d.Set("instance_name", instanceName); err != nil {
// 		return fmt.Errorf("Error setting instance_name: %s", err)
// 	}

// 	defer c.Close()

// 	gcPolicy, err := generateBigtableGCPolicy(d)
// 	if err != nil {
// 		return err
// 	}

// 	tableName := d.Get("table").(string)
// 	columnFamily := d.Get("column_family").(string)

// 	retryFunc := func() (interface{}, error) {
// 		reqErr := c.SetGCPolicy(ctx, tableName, columnFamily, gcPolicy)
// 		return "", reqErr
// 	}
// 	// The default create timeout is 20 minutes.
// 	timeout := d.Timeout(schema.TimeoutCreate)
// 	pollInterval := time.Duration(30) * time.Second
// 	// Mutations to gc policies can only happen one-at-a-time and take some amount of time.
// 	// Use a fixed polling rate of 30s based on the RetryInfo returned by the server rather than
// 	// the standard up-to-10s exponential backoff for those operations.
// 	_, err = retryWithPolling(retryFunc, timeout, pollInterval, isBigTableRetryableError)
// 	if err != nil {
// 		return err
// 	}

// 	table, err := c.TableInfo(ctx, tableName)
// 	if err != nil {
// 		return fmt.Errorf("Error retrieving table. Could not find %s in %s. %s", tableName, instanceName, err)
// 	}

// 	for _, i := range table.FamilyInfos {
// 		if i.Name == columnFamily {
// 			d.SetId(i.GCPolicy)
// 		}
// 	}

// 	return resourceBigtableGCPolicyRead(d, meta)
// }

// func resourceBigtableGCPolicyRead(d *schema.ResourceData, meta interface{}) error {
// 	config := meta.(*Config)
// 	userAgent, err := generateUserAgentString(d, config.userAgent)
// 	if err != nil {
// 		return err
// 	}
// 	ctx := context.Background()

// 	project, err := getProject(d, config)
// 	if err != nil {
// 		return err
// 	}

// 	instanceName := GetResourceNameFromSelfLink(d.Get("instance_name").(string))
// 	c, err := config.BigTableClientFactory(userAgent).NewAdminClient(project, instanceName)
// 	if err != nil {
// 		return fmt.Errorf("Error starting admin client. %s", err)
// 	}

// 	defer c.Close()

// 	name := d.Get("table").(string)
// 	columnFamily := d.Get("column_family").(string)
// 	ti, err := c.TableInfo(ctx, name)
// 	if err != nil {
// 		log.Printf("[WARN] Removing %s because it's gone", name)
// 		d.SetId("")
// 		return nil
// 	}

// 	for _, fi := range ti.FamilyInfos {
// 		if fi.Name != columnFamily {
// 			continue
// 		}

// 		d.SetId(fi.GCPolicy)

// 		// No GC Policy.
// 		if fi.FullGCPolicy.String() == "" {
// 			return nil
// 		}

// 		// Only set `gc_rules`` when the legacy fields are not set. We are not planning to support legacy fields.
// 		maxAge := d.Get("max_age")
// 		maxVersion := d.Get("max_version")
// 		if d.Get("mode") == "" && len(maxAge.([]interface{})) == 0 && len(maxVersion.([]interface{})) == 0 {
// 			gcRuleString, err := gcPolicyToGCRuleString(fi.FullGCPolicy, true)
// 			if err != nil {
// 				return err
// 			}
// 			gcRuleJsonString, err := json.Marshal(gcRuleString)
// 			if err != nil {
// 				return fmt.Errorf("Error marshaling GC policy to json: %s", err)
// 			}
// 			d.Set("gc_rules", string(gcRuleJsonString))
// 		}
// 		break
// 	}

// 	if err := d.Set("project", project); err != nil {
// 		return fmt.Errorf("Error setting project: %s", err)
// 	}

// 	return nil
// }

// // Recursively convert Bigtable GC policy to JSON format in a map.
// func gcPolicyToGCRuleString(gc bigtable.GCPolicy, topLevel bool) (map[string]interface{}, error) {
// 	result := make(map[string]interface{})
// 	switch bigtable.GetPolicyType(gc) {
// 	case bigtable.PolicyMaxAge:
// 		age := gc.(bigtable.MaxAgeGCPolicy).GetDurationString()
// 		if topLevel {
// 			rule := make(map[string]interface{})
// 			rule["max_age"] = age
// 			rules := []interface{}{}
// 			rules = append(rules, rule)
// 			result["rules"] = rules
// 		} else {
// 			result["max_age"] = age
// 		}
// 		break
// 	case bigtable.PolicyMaxVersion:
// 		// bigtable.MaxVersionsGCPolicy is an int.
// 		// Not sure why max_version is a float64.
// 		// TODO: Maybe change max_version to an int.
// 		version := float64(int(gc.(bigtable.MaxVersionsGCPolicy)))
// 		if topLevel {
// 			rule := make(map[string]interface{})
// 			rule["max_version"] = version
// 			rules := []interface{}{}
// 			rules = append(rules, rule)
// 			result["rules"] = rules
// 		} else {
// 			result["max_version"] = version
// 		}
// 		break
// 	case bigtable.PolicyUnion:
// 		result["mode"] = "union"
// 		rules := []interface{}{}
// 		for _, c := range gc.(bigtable.UnionGCPolicy).Children {
// 			gcRuleString, err := gcPolicyToGCRuleString(c, false)
// 			if err != nil {
// 				return nil, err
// 			}
// 			rules = append(rules, gcRuleString)
// 		}
// 		result["rules"] = rules
// 		break
// 	case bigtable.PolicyIntersection:
// 		result["mode"] = "intersection"
// 		rules := []interface{}{}
// 		for _, c := range gc.(bigtable.IntersectionGCPolicy).Children {
// 			gcRuleString, err := gcPolicyToGCRuleString(c, false)
// 			if err != nil {
// 				return nil, err
// 			}
// 			rules = append(rules, gcRuleString)
// 		}
// 		result["rules"] = rules
// 	default:
// 		break
// 	}

// 	if err := validateNestedPolicy(result, topLevel); err != nil {
// 		return nil, err
// 	}

// 	return result, nil
// }

// func generateBigtableGCPolicy(d *schema.ResourceData) (bigtable.GCPolicy, error) {
// 	var policies []bigtable.GCPolicy
// 	mode := d.Get("mode").(string)
// 	ma, aok := d.GetOk("max_age")
// 	mv, vok := d.GetOk("max_version")
// 	gcRules, gok := d.GetOk("gc_rules")

// 	if !aok && !vok && !gok {
// 		return bigtable.NoGcPolicy(), nil
// 	}

// 	if mode == "" && aok && vok {
// 		return nil, fmt.Errorf("if multiple policies are set, mode can't be empty")
// 	}

// 	if gok {
// 		var topLevelPolicy map[string]interface{}
// 		if err := json.Unmarshal([]byte(gcRules.(string)), &topLevelPolicy); err != nil {
// 			return nil, err
// 		}
// 		return getGCPolicyFromJSON(topLevelPolicy /*isTopLevel=*/, true)
// 	}

// 	if aok {
// 		l, _ := ma.([]interface{})
// 		d, err := getMaxAgeDuration(l[0].(map[string]interface{}))
// 		if err != nil {
// 			return nil, err
// 		}

// 		policies = append(policies, bigtable.MaxAgePolicy(d))
// 	}

// 	if vok {
// 		l, _ := mv.([]interface{})
// 		n, _ := l[0].(map[string]interface{})["number"].(int)

// 		policies = append(policies, bigtable.MaxVersionsPolicy(n))
// 	}

// 	switch mode {
// 	case GCPolicyModeUnion:
// 		return bigtable.UnionPolicy(policies...), nil
// 	case GCPolicyModeIntersection:
// 		return bigtable.IntersectionPolicy(policies...), nil
// 	}

// 	return policies[0], nil
// }

// func getGCPolicyFromJSON(inputPolicy map[string]interface{}, isTopLevel bool) (bigtable.GCPolicy, error) {
// 	policy := []bigtable.GCPolicy{}

// 	if err := validateNestedPolicy(inputPolicy, isTopLevel); err != nil {
// 		return nil, err
// 	}

// 	for _, p := range inputPolicy["rules"].([]interface{}) {
// 		childPolicy := p.(map[string]interface{})
// 		if err := validateNestedPolicy(childPolicy /*isTopLevel=*/, false); err != nil {
// 			return nil, err
// 		}

// 		if childPolicy["max_age"] != nil {
// 			maxAge := childPolicy["max_age"].(string)
// 			duration, err := time.ParseDuration(maxAge)
// 			if err != nil {
// 				return nil, fmt.Errorf("invalid duration string: %v", maxAge)
// 			}
// 			policy = append(policy, bigtable.MaxAgePolicy(duration))
// 		}

// 		if childPolicy["max_version"] != nil {
// 			version := childPolicy["max_version"].(float64)
// 			policy = append(policy, bigtable.MaxVersionsPolicy(int(version)))
// 		}

// 		if childPolicy["mode"] != nil {
// 			n, err := getGCPolicyFromJSON(childPolicy /*isTopLevel=*/, false)
// 			if err != nil {
// 				return nil, err
// 			}
// 			policy = append(policy, n)
// 		}
// 	}

// 	switch inputPolicy["mode"] {
// 	case strings.ToLower(GCPolicyModeUnion):
// 		return bigtable.UnionPolicy(policy...), nil
// 	case strings.ToLower(GCPolicyModeIntersection):
// 		return bigtable.IntersectionPolicy(policy...), nil
// 	default:
// 		return policy[0], nil
// 	}
// }

// func validateNestedPolicy(p map[string]interface{}, isTopLevel bool) error {
// 	if len(p) > 2 {
// 		return fmt.Errorf("rules has more than 2 fields")
// 	}
// 	maxVersion, maxVersionOk := p["max_version"]
// 	maxAge, maxAgeOk := p["max_age"]
// 	rulesObj, rulesOk := p["rules"]

// 	_, modeOk := p["mode"]
// 	rules, arrOk := rulesObj.([]interface{})
// 	_, vCastOk := maxVersion.(float64)
// 	_, aCastOk := maxAge.(string)

// 	if rulesOk && !arrOk {
// 		return fmt.Errorf("`rules` must be array")
// 	}

// 	if modeOk && len(rules) < 2 {
// 		return fmt.Errorf("`rules` need at least 2 GC rule when mode is specified")
// 	}

// 	if isTopLevel && !rulesOk {
// 		return fmt.Errorf("invalid nested policy, need `rules`")
// 	}

// 	if isTopLevel && !modeOk && len(rules) != 1 {
// 		return fmt.Errorf("when `mode` is not specified, `rules` can only have 1 child rule")
// 	}

// 	if !isTopLevel && len(p) == 2 && (!modeOk || !rulesOk) {
// 		return fmt.Errorf("need `mode` and `rules` for child nested policies")
// 	}

// 	if !isTopLevel && len(p) == 1 && !maxVersionOk && !maxAgeOk {
// 		return fmt.Errorf("need `max_version` or `max_age` for the rule")
// 	}

// 	if maxVersionOk && !vCastOk {
// 		return fmt.Errorf("`max_version` must be a number")
// 	}

// 	if maxAgeOk && !aCastOk {
// 		return fmt.Errorf("`max_age must be a string")
// 	}

// 	return nil
// }

// func getMaxAgeDuration(values map[string]interface{}) (time.Duration, error) {
// 	d := values["duration"].(string)
// 	if d != "" {
// 		return time.ParseDuration(d)
// 	}

// 	days := values["days"].(int)

// 	return time.Hour * 24 * time.Duration(days), nil
// }

func generateBigtableFamilyGCPolicy(d map[string]interface{}) (bigtable.GCPolicy, error) {
	var policies []bigtable.GCPolicy

	// for _, v := range d {
	binding := d
	// binding := v.(map[string]interface{})
	fmt.Println(">>>> binding :", binding)
	mode := binding["mode"].(string)
	fmt.Println(">>>> mode :", mode)
	ma := binding["max_age"].([]interface{})
	fmt.Println(">>>> ma :", ma)
	mv := binding["max_version"].([]interface{})
	fmt.Println(">>>> mv :", mv)
	gcRules := binding["gc_rules"].(string)
	fmt.Println(">>>> gcRules :", gcRules)
	// }

	// mode := "mode"
	// ma := "max_age"
	// mv := "max_version"
	// gcRules := "gc_rules"
	fmt.Println(">>>>>>>>>>><generateBigtableFamilyGCPolicy ")
	fmt.Println("mode :", mode)
	fmt.Println("ma :", ma)
	fmt.Println("mv :", mv)
	fmt.Println("gcRules :", gcRules)

	// if !aok && !vok && !gok {
	// 	return bigtable.NoGcPolicy(), nil
	// }

	if len(ma) == 0 && len(mv) == 0 && gcRules == "" {
		fmt.Println("No GC Policy SET")
		return bigtable.NoGcPolicy(), nil
	}

	// if mode == "" && aok && vok {
	// 	return nil, fmt.Errorf("if multiple policies are set, mode can't be empty")
	// }

	if mode == "" && len(ma) > 0 && len(mv) > 0 {
		fmt.Println("MODE is empty")
		return nil, fmt.Errorf("if multiple policies are set, mode can't be empty")
	}

	// if gok {
	// 	var topLevelPolicy map[string]interface{}
	// 	if err := json.Unmarshal([]byte(gcRules.(string)), &topLevelPolicy); err != nil {
	// 		return nil, err
	// 	}
	// 	return getGCPolicyFromJSON(topLevelPolicy /*isTopLevel=*/, true)
	// }
	if gcRules != "" {
		fmt.Println("gcRules is not empty")
		var topLevelPolicy map[string]interface{}
		if err := json.Unmarshal([]byte(gcRules), &topLevelPolicy); err != nil {
			return nil, err
		}
		return getGCPolicyFromJSON(topLevelPolicy /*isTopLevel=*/, true)
	}

	// if aok {
	// 	l, _ := ma.([]interface{})
	// 	d, err := getMaxAgeDuration(l[0].(map[string]interface{}))
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	policies = append(policies, bigtable.MaxAgePolicy(d))
	// }

	if len(ma) > 0 {
		fmt.Println("max_age is not empty")
		l, _ := ma[0].(map[string]interface{})
		fmt.Println("max_age---l :", l)
		d, err := getMaxAgeDuration(l)
		if err != nil {
			return nil, err
		}
		policies = append(policies, bigtable.MaxAgePolicy(d))
	}

	// if vok {
	// 	l, _ := mv.([]interface{})
	// 	n, _ := l[0].(map[string]interface{})["number"].(int)

	// 	policies = append(policies, bigtable.MaxVersionsPolicy(n))
	// }

	if len(mv) > 0 {
		fmt.Println("max_version is not empty")
		l, _ := mv[0].(map[string]interface{})
		n, _ := l["number"].(int)

		policies = append(policies, bigtable.MaxVersionsPolicy(n))
	}

	switch mode {
	case GCPolicyModeUnion:
		return bigtable.UnionPolicy(policies...), nil
	case GCPolicyModeIntersection:
		return bigtable.IntersectionPolicy(policies...), nil
	}

	return policies[0], nil
}
