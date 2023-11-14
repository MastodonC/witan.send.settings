(ns witan.send.settings
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [clojure.java.io :as io]
            [clojure.string  :as string]
            [tablecloth.api  :as tc]
            [tech.v3.dataset :as ds]
            [witan.gias :as gias]))



;;; # Utility functions
(defn- compare-mapped-keys
  [m k1 k2]
  (compare [(get m k1) k1]
           [(get m k2) k2]))

(defn ds->hash-map
  "Given dataset `ds`, returns hash-map with
   - keys from the `key-cols` of `ds`.
   - vals from the remaining columns of `ds`,
     or just the `val-cols` if specified.
  If `key-cols` (or `val-cols`) specify multiple columns,
  then the keys (or vals) are returned as maps keyed by column name."
  [ds key-cols & {:keys [val-cols]}]
  (let [columns (fn [ds] (if (< 1 (tc/column-count ds))
                           (tc/rows ds :as-maps)
                           (-> (tc/columns ds :as-seqs) first)))
        ks       (-> ds
                     (tc/select-columns key-cols)
                     (columns))
        vs       (-> ds
                     ((fn [ds] (if val-cols
                                 (tc/select-columns ds val-cols)
                                 (tc/drop-columns ds key-cols))))
                     (columns))]
    (zipmap ks vs)))

(defn ds->sorted-map-by
  "Given dataset `ds`, returns sorted-map with
   - keys from the `key-col` of `ds` (values of which must compare).
   - vals from the remaining columns,
     or from just the `val-cols` if specified.
   - ordered by the order of rows in the `ds`,
     or by the values of `:order-col` if specified (which must compare).
  If `val-cols` specify multiple columns,
  then the vals are returned as maps keyed by column name."
  [ds key-col & {:keys [val-cols order-col]}]
  (let [columns (fn [ds] (if (< 1 (tc/column-count ds))
                           (tc/rows ds :as-maps)
                           (-> (tc/columns ds :as-seqs) first)))
        ks       (-> ds
                     (tc/select-columns key-col)
                     (columns))
        vs       (-> ds
                     ((fn [ds] (if val-cols
                                 (tc/select-columns ds val-cols)
                                 (tc/drop-columns ds key-col))))
                     (columns))
        os       (or
                  (get ds order-col)
                  (range))]
    (into (sorted-map-by (partial compare-mapped-keys (zipmap ks os)))
          (zipmap ks vs))
    )
  )

(comment ;; test
  (-> (tc/dataset [{:k1 :k1-1 :k2 :k2-1 :order 2 :v1 1 :v2 2}
                   {:k1 :k1-2 :k2 :k2-2 :order 1 :v1 2 :v2 1}])
      (ds->hash-map [:k1 :k2] :val-cols [:v1 #_:v2])
      )

  (-> (tc/dataset [{:k1 :k1-1 :k2 :k2-1 :order 2 :v1 1 :v2 2}
                   {:k1 :k1-2 :k2 :k2-2 :order 1 :v1 2 :v2 1}])
      (ds->sorted-map-by [:k1 #_:k2] :val-cols [:v1 :v2] :order-col :order)
      )

  )

(defn map->ds
  "Given map with maps as values `m`, returns dataset
   with keys and vals of vals in columns
   named per the keys prefixed with `key-prefix`."
  ;; TODO: Expand to unpack keys that are maps.
  [m key-prefix]
  (->> m
       (reduce-kv (fn [vec-of-rows-as-maps k v]
                    (conj vec-of-rows-as-maps
                          (merge {(keyword key-prefix) k}
                                 (update-keys v (comp keyword (partial str key-prefix "-") name)))))
                  [])
       tc/dataset))



;;; # Setting Definitions
(defn- filepath
  "Parse `filename`, `dir` & `resource-dir` parameters specifying definitions file to read:
   - if `filename` is specified with a path (i.e. contains a \"/\"), then use that filepath, otherwise
   - if `dir` is specified (non-nil), then prepend to `filename` to get filepath, otherwise
   - if `resource-dir` is specified (non-nil), then prepend to `filename` and open resource-dir file on that path,
   - else returns nil."
  [filename dir resource-dir]
  (cond
    (re-find #"\/" filename) filename
    (some? dir)              (str dir filename)
    (some? resource-dir)     (io/resource (str resource-dir filename))
    :else                    nil))

(def csv->ds-opts
  "Common options for `ds/->dataset` read of setting defs CSV files."
  {:file-type        :csv
   :separator        ","
   :header-row?      :true
   :key-fn           keyword
   :parser-fn        {:abbreviation :string
                      :order        :int16
                      :name         :string
                      :label        :string
                      :definition   :string}})

;;; ## `:estab-cat`: Establishment Categories
(defn estab-cats-ds
  "Dataset of establishment category definitions.
   Read from CSV file specified by `::estab-cats-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::estab-cats-ds` val."
  [& {estab-cats-ds' ::estab-cats-ds
      ::keys         [estab-cats-filename
                      dir
                      resource-dir]
      :or            {estab-cats-filename "estab-cats.csv"}}]
  (or estab-cats-ds'
      (let [filepath (filepath estab-cats-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in (merge-with merge
                                         csv->ds-opts
                                         {:parser-fn {:designate?  :boolean
                                                      :split-area? :boolean}})))))
      (tc/dataset)))

(comment ;; test
  (estab-cats-ds)
  (estab-cats-ds ::resource-dir "standard/")
  
  (estab-cats-ds {::estab-cats-filename "estab-cats-test.csv"
                  ::dir                 "./tmp/"})

  (estab-cats-ds {::estab-cats-filename "./tmp/estab-cats-test.csv"
                  ::dir                 "./tmp/"
                  ::resource-dir        "standard/"})

  (estab-cats-ds ::estab-cats-ds "override")

  )

(defn estab-cats
  "Map establishment category abbreviations to attributes.
   Derived from `(estab-cats-ds cfg)` unless specified in truthy `::estab-cats` val."
  [& {estab-cats' ::estab-cats
      :as         cfg}]
  (or estab-cats'
      (-> (estab-cats-ds cfg)
          (ds->sorted-map-by :abbreviation :order-col :order))))

(comment ;; test
  (estab-cats)
  (estab-cats ::resource-dir "standard/")
  (estab-cats ::estab-cats "override")
  (estab-cats ::estab-cats-ds (estab-cats-ds {::estab-cats-filename "./tmp/estab-cats-test.csv"}))

  )


;;; ## `:designation`: Designations
(defn designations-ds
  "Dataset of designation definitions.
   Read from CSV file specified by `::designations-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::designations-ds` val."
  [& {designations-ds' ::designations-ds
      ::keys           [designations-filename
                        dir
                        resource-dir]
      :or              {designations-filename "designations.csv"}}]
  (or designations-ds'
      (let [filepath (filepath designations-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in csv->ds-opts))))
      (tc/dataset)))

(comment ;; test
  (designations-ds)
  (designations-ds ::resource-dir "standard/")
  (designations-ds {::designations-filename "designations-test.csv"
                    ::dir                   "./tmp/"})
  (designations-ds {::designations-filename "./tmp/designations-test.csv"
                    ::resource-dir          "standard/"})

  )

(defn designations
  "Map designation abbreviations to attributes.
   Derived from `(designations-ds cfg)` unless specified in truthy `::designations` val."
  [& {designations' ::designations
      :as           cfg}]
  (or designations'
      (-> (designations-ds cfg)
          (ds->sorted-map-by :abbreviation :order-col :order))))

(comment ;; test
  (designations)
  (designations ::resource-dir "standard/")
  (designations ::designations-ds (designations-ds {::designations-filename "./tmp/designations-test.csv"}))

  )


;;; ## `:area`: Area
(defn areas-ds
  "Dataset of area definitions.
   Read from CSV file specified by `::areas-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::areas-ds` val."
  [& {areas-ds' ::areas-ds
      ::keys    [areas-filename
                 dir
                 resource-dir]
      :or       {areas-filename "areas.csv"}}]
  (or areas-ds'
      (let [filepath (filepath areas-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in csv->ds-opts))))
      (tc/dataset)))

(comment ;; test
  (areas-ds)
  (areas-ds ::resource-dir "standard/")
  (areas-ds {::areas-filename "areas-test.csv"
             ::dir            "./tmp/"})
  (areas-ds {::areas-filename "./tmp/areas-test.csv"
             ::resource-dir   "standard/"})

  )


(defn areas
  "Map area abbreviations to attributes.
  Derived from `(areas-ds cfg)` unless specified in truthy `::areas` val."
  [& {areas' ::areas
      :as    cfg}]
  (or areas'
      (-> (areas-ds cfg)
          (ds->sorted-map-by :abbreviation :order-col :order))))

(comment ;; test
  (areas)
  (areas ::resource-dir "standard/")
  (areas ::areas-ds (areas-ds {::areas-filename "./tmp/areas-test.csv"}))

  )




;;; ## Settings
;; Combining `:estab-cat`, `:designation` and `:area`:
(defn settings
  "Map setting abbreviations to attributes.
   Derived from estab-cat, designation & area definitions unless specified in truthy `::settings` val:
   - estab-cat definitions from `(estab-cats cfg)`
   - designation definitions from `(designations cfg)`
   - area definitions from `(areas cfg)`"
  [& {settings' ::settings
      :as       cfg}]
  (or settings'
      (let [;; Derive definition datasets from maps in case maps have been provided in `cfg`
            ;; TODO: If maps not provided then use ds directly? Move map->ds to the ds fns if map provided?
            estab-cats'      (estab-cats cfg)
            designations'    (designations cfg)
            areas'           (areas cfg)
            estab-cats-ds'   (map->ds estab-cats' "estab-cat")
            designations-ds' (map->ds designations' "designation")
            areas-ds'        (map->ds areas' "area")]
        ;; TODO: Ensure works when not designating or area-splitting.
        ;; TODO: Only construct name|label|definition if have all components needed (i.e. need area if :estab-cat-split-area).
        (-> estab-cats-ds'
            ;; Expand `:estab-cat`s to be designated by designations
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-designate?))
                                   (-> (tc/select-rows $ (comp :estab-cat-designate?))
                                       (tc/cross-join designations-ds'))))
            ;; Expand out `:estab-cat`s to be split by area
            (as-> $
                (tc/concat-copying (tc/drop-rows $ (comp :estab-cat-split-area?))
                                   (-> (tc/select-rows $ (comp :estab-cat-split-area?))
                                       (tc/cross-join areas-ds'))))
            ;; Derive setting `:abbreviation`
            (tc/map-columns :abbreviation [:estab-cat :designation :area]
                            (fn [& args] (clojure.string/join "_" (filter some? args))))
            (tc/order-by [:estab-cat-order :designation-order :area-order])
            (tc/add-column :order (iterate inc 1))
            ;; Derive setting attributes from corresponding estab-cat, designation & area attributes (if available)
            (tc/map-rows (fn [{:keys [estab-cat-name       designation-name       area-name
                                      estab-cat-label      designation-label      area-label
                                      estab-cat-definition designation-definition area-definition]}]
                           (let [setting-attr-f (fn [estab-cat-attr designation-attr area-attr]
                                                  (str estab-cat-attr
                                                       (when designation-attr (format " - %s" designation-label))
                                                       (when area-attr (format " [%s]" area-label))))]
                             (merge (when estab-cat-name
                                      {:name       (setting-attr-f estab-cat-name       designation-name       area-name      )})
                                    (when estab-cat-label
                                      {:label      (setting-attr-f estab-cat-label      designation-label      area-label     )})
                                    (when estab-cat-definition
                                      {:definition (setting-attr-f estab-cat-definition designation-definition area-definition)})))))
            ;; Tidy dataset
            (tc/select-columns [:abbreviation :order :name :label :definition :estab-cat :designation :area])
            ;; Convert to map
            (ds->sorted-map-by :abbreviation :order-col :order)))))

(comment ;; test
  (-> {::resource-dir "standard/"}
      #_((fn [m] (assoc m
                        ::estab-cats-ds (-> (estab-cats-ds m)
                                            (tc/drop-columns [:name]))
                        #_#_::designations-ds (-> (designations-ds m)
                                                  (tc/drop-columns [:name :order]))
                        #_#_::areas-ds (-> (areas-ds m)
                                           (tc/drop-columns [:name :order]))
                        )))
      (settings)
      #_(tc/drop-columns #"^.*definition")
      #_(vary-meta assoc :print-index-range 1000)
      )

  )


(defn setting-split-regexp
  "Return regex pattern for splitting setting abbreviations into components.
   Relies on area abbreviations not clashing with designation abbreviations.
   Collection of areas can be specified (first supplied is used):
   - directly via `:area-abbreviations` key
   - via keys of areas map specified in `:areas` key
   or (if not) is via keys of areas map derived from settings cfg via `(areas cfg)`."
  [& {setting-split-regex' ::setting-split-regex
      area-abbreviations   :area-abbreviations ; Note deliberately NOT namespaced
      areas'               ::areas
      :as                  cfg}]
  (or setting-split-regex'
      (let [area-abbreviations' (or area-abbreviations
                                    (keys (or areas'
                                              (areas cfg))))]
        (re-pattern (str "^(?<estabCat>[^_]+)"
                         "_??(?<designation>[^_]+)??"
                         "_?(?<area>" (string/join "|" area-abbreviations') ")?$")))))

(comment ;; test
  (setting-split-regexp ::setting-split-regex "42")
  (setting-split-regexp :area-abbreviations ["foo" "bar"])
  (setting-split-regexp ::resource-dir "standard/")
  (-> (settings ::resource-dir "standard/")
      keys
      (as-> $ (tc/dataset {:setting $}))
      (tc/separate-column :setting [:estab-cat :designation :area-indicator]
                          (setting-split-regexp ::resource-dir "standard/")
                          {:drop-column? false})
      (vary-meta assoc :print-index-range 1000)
      )

  )




;;; # Settings
(def sen2-estab-keys
  "SEN2 establishment column keywords from `placement-detail` table"
  [:urn :ukprn :sen-unit-indicator :resourced-provision-indicator :sen-setting])

;;; ## Manual and Override settings
(defn sen2-estab-settings-manual-ds
  "Dataset mapping SEN2 Estab keys to manual settings.
   Read from CSV file specified by `::sen2-estab-settings-manual-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::sen2-estab-settings-manual-ds` val.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {sen2-estab-settings-manual-ds' ::sen2-estab-settings-manual-ds
      ::keys                        [sen2-estab-settings-manual-filename
                                     dir
                                     resource-dir]
      :or                           {sen2-estab-settings-manual-filename "sen2-estab-settings-manual.csv"}}]
  (or sen2-estab-settings-manual-ds'
      (let [filepath (filepath sen2-estab-settings-manual-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :column-blocklist ["reference-website"
                                                 "notes"]
                              :key-fn      keyword
                              :parser-fn   {:urn                           :string
                                            :ukprn                         :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string
                                            :designation                   :string
                                            :la-code                       :string}}))))
      (tc/dataset)))

(comment ;; test
  (sen2-estab-settings-manual-ds)
  (-> (sen2-estab-settings-manual-ds ::resource-dir "standard/")
      ((fn [ds] (-> ds tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing]))))
      )

  )

(defn sen2-estab-settings-manual
  "Map SEN2 Estab keys to manual settings.
   Derived from `(sen2-estab-settings-manual-ds cfg)` unless specified in truthy `::sen2-estab-settings-manual` val."
  [& {sen2-estab-settings-manual' ::sen2-estab-settings-manual
      :as                      cfg}]
  (or sen2-estab-settings-manual'
      (-> (sen2-estab-settings-manual-ds cfg)
          (ds->hash-map sen2-estab-keys))))

(comment ;; test
  (sen2-estab-settings-manual)
  (-> (sen2-estab-settings-manual ::resource-dir "standard/")
      )

  )


(defn sen2-estab-settings-override-ds
  "Dataset mapping SEN2 Estab keys to override settings.
   Read from CSV file specified by `::sen2-estab-settings-override-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::sen2-estab-settings-override-ds` val.
  (SEN2 Estab = URN|UKPRN split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {sen2-estab-settings-override-ds' ::sen2-estab-settings-override-ds
      ::keys                        [sen2-estab-settings-override-filename
                                     dir
                                     resource-dir]
      :or                           {sen2-estab-settings-override-filename "sen2-estab-settings-override.csv"}}]
  (or sen2-estab-settings-override-ds'
      (let [filepath (filepath sen2-estab-settings-override-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :column-blocklist ["reference-website"
                                                 "notes"]
                              :key-fn      keyword
                              :parser-fn   {:urn                           :string
                                            :ukprn                         :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string
                                            :designation                   :string
                                            :la-code                       :string}}))))
      (tc/dataset)))

(comment ;; test
  (sen2-estab-settings-override-ds)
  (-> (sen2-estab-settings-override-ds ::dir "./tmp/")
      #_((fn [ds] (-> ds tc/info (tc/select-columns [:col-name :datatype :n-valid :n-missing]))))
      )

  )

(defn sen2-estab-settings-override
  "Map SEN2 Estab keys to override settings.
   Derived from `(sen2-estab-settings-override-ds cfg)` unless specified in truthy `::sen2-estab-settings-override` val."
  [& {sen2-estab-settings-override' ::sen2-estab-settings-override
      :as                      cfg}]
  (or sen2-estab-settings-override'
      (-> (sen2-estab-settings-override-ds cfg)
          (ds->hash-map sen2-estab-keys))))

(comment ;; test
  (sen2-estab-settings-override)
  (-> (sen2-estab-settings-override ::sen2-estab-settings-override-filename "./tmp/sen2-estab-settings-override-test.csv")
      )

  )

;;; ## GIAS Establishment Type to `:estab-cat`
(def estab-type-keys [:type-of-establishment-name
                      :sen-unit-indicator
	              :resourced-provision-indicator
	              :sen-setting])

(defn estab-type-to-estab-cat-ds
  "Dataset mapping Estab Types to `:estab-cat` Estab Categories categories.
   Read from CSV file specified by `::estab-type-to-estab-cat-filename`, `::dir` & `::resource-dir` vals,
   unless supplied in truthy `::estab-type-to-estab-cat-ds` val.
  (Estab Type = GIAS Establishment Types split by SENU|RP augmented by SEN2 <SENsetting>s)"
  [& {estab-type-to-estab-cat-ds' ::estab-type-to-estab-cat-ds
      ::keys                      [estab-type-to-estab-cat-filename
                                   dir
                                   resource-dir]
      :or                         {estab-type-to-estab-cat-filename "estab-type-to-estab-cat.csv"}}]
  (or estab-type-to-estab-cat-ds'
      (let [filepath (filepath estab-type-to-estab-cat-filename dir resource-dir)]
        (when filepath
          (with-open [in (-> filepath
                             io/file
                             io/input-stream)]
            (ds/->dataset in {:file-type   :csv
                              :separator   ","
                              :header-row? :true
                              :key-fn      keyword
                              :parser-fn   {:type-of-establishment-name    :string
	                                    :sen-unit-indicator            :boolean
	                                    :resourced-provision-indicator :boolean
	                                    :sen-setting                   :string
	                                    :estab-cat                     :string}}))))
      (tc/dataset)))

(comment ;; test
  (estab-type-to-estab-cat-ds)
  (-> (estab-type-to-estab-cat-ds ::resource-dir "standard/")
      )

  )


(defn estab-type-to-estab-cat
  "Map Estab Types to `:estab-cat` Estab Categories categories.
   Derived from `(estab-type-to-estab-cat-ds cfg)` unless specified in truthy `::estab-type-to-estab-cat` val."
  [& {estab-type-to-estab-cat' ::estab-type-to-estab-cat
      :as                      cfg}]
  (or estab-type-to-estab-cat'
      (-> (estab-type-to-estab-cat-ds cfg)
          (ds->hash-map estab-type-keys))))

(comment ;; test
  (estab-type-to-estab-cat)
  (-> (estab-type-to-estab-cat ::resource-dir "standard/")
      )

  )



(comment ;; dev
  ;; GIAS
  ;; :estab -> :estab-name & :estab-type
  ;; GIAS urn return map with :establishment-name, :establishment-type


  (-> (gias/edubaseall-send->ds)
      tc/column-names
      )

  (
   (fn [{:keys [urn
                ukprn
                sen-unit-indicator
                resourced-provision-indicator
                sen-setting]
         :or   {urn                           nil
                ukprn                         nil
                sen-unit-indicator            false
                resourced-provision-indicator false
                sen-setting                   nil}
         :as   sen2-estab}
        & {::keys [sen-unit-name
                   resourced-provision-name]
           :or    {sen-unit-name            "(SEN Unit)"
                   resourced-provision-name "(Resourced Provision)"}
           :as    cfg}]
     (let [;; Get GIAS information for this sen2-estab
           edubaseall-send ((gias/edubaseall-send->map) urn)
           estab-name-gias (let [establishment-name (:establishment-name edubaseall-send)]
                             (when establishment-name
                               (str establishment-name
                                    (when sen-unit-indicator (str " " sen-unit-name))
                                    (when resourced-provision-indicator (str " " resourced-provision-name)))))
           estab-type-gias (let [type-of-establishment-name (:type-of-establishment-name edubaseall-send)]
                             (when type-of-establishment-name
                               (get (estab-type-to-estab-cat cfg)
                                    {:type-of-establishment-name    type-of-establishment-name
                                     :sen-unit-indicator            sen-unit-indicator
                                     :resourced-provision-indicator resourced-provision-indicator
                                     :sen-setting                   sen-setting})))
           ]
       {#_#_:edubaseall-send edubaseall-send
        :estab-name-gias     estab-name-gias
        :estab-type-gias     estab-type-gias}
       )
     )
   {:urn                               "144009"
    #_#_:ukprn                         nil
    #_#_:sen-unit-indicator            false
    #_#_:resourced-provision-indicator false
    #_#_:sen-setting                   nil}
   {::resource-dir             "standard/"
    ::sen-unit-name            "Access Centre"
    ::resourced-provision-name "Resource Base"}
   )




  )
